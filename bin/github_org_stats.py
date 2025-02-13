"""
GitHub Organization Statistics Reporter
Fetches and displays various metrics for a GitHub organization using the GitHub API.
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
import dask.bag as db
from dask.distributed import Client
import requests

# pylint: disable=broad-exception-caught

ARG = None

class GitHubOrgStats:
    ''' GitHub organization statistics
        Ititilization:
          token -- GitHub API token
          org_name -- GitHub organization name
          days -- Number of days to look back
          headers -- GitHub API headers
          base_url -- GitHub API base URL
    '''
    def __init__(self, token, org_name):
        self.token = token
        self.org_name = org_name
        self.days = 7
        self.headers = {'Authorization': f'token {token}',
                        'Accept': 'application/vnd.github.v3+json'
                       }
        self.base_url = 'https://api.github.com'

    def get_cutoff_date(self):
        """Get cutoff date based on configured days"""
        return datetime.now() - timedelta(days=self.days)

    def call_github_api(self, suffix, params=None):
        """Call the GitHub API with the given URL and parameters"""
        url = f"{self.base_url}/{suffix}"
        response = requests.get(url, headers=self.headers, params=params, timeout=10)
        if response.status_code != 200:
            raise requests.RequestException(f"Failed to fetch {suffix}: {response.status_code}")
        return response

    def get_repos(self):
        """Fetch all repositories for the organization"""
        print(f"Getting repositories for {self.org_name}")
        repos = []
        page = 1
        while True:
            response = self.call_github_api(f"orgs/{self.org_name}/repos",
                                            params={'page': page, 'per_page': 100})
            page_repos = response.json()
            if not page_repos:
                break
            repos.extend(page_repos)
            page += 1
        return repos

    def get_members(self):
        """Fetch all members of the organization"""
        print(f"Getting members for {self.org_name}")
        members = []
        page = 1
        while True:
            response = self.call_github_api(f"orgs/{self.org_name}/members",
                                            params={'page': page, 'per_page': 100})
            page_members = response.json()
            if not page_members:
                break
            members.extend(page_members)
            page += 1
        return members

    def get_commit_activity(self, repo_name):
        """Get commit activity for a repository over the last year"""
        print(f"Getting commit activity for {repo_name}")
        response = self.call_github_api(f"repos/{self.org_name}/{repo_name}/stats/commit_activity")
        if response.status_code != 200:
            return None
        return response.json()

    def get_pull_stats(self, repo_name):
        """Get pull request statistics for a repository"""
        print(f"Getting pull requests for {repo_name}")
        pulls = []
        page = 1
        cutoff_date = self.get_cutoff_date()
        while True:
            response = self.call_github_api(f"repos/{self.org_name}/{repo_name}/pulls",
                                            params={'state': 'all', 'page': page, 'per_page': 100,
                                                    'sort': 'updated', 'direction': 'desc'})
            if response.status_code != 200:
                return None
            page_pulls = response.json()
            if not page_pulls:
                break
            # Filter PRs updated in last 7 days
            recent_pulls = [pr for pr in page_pulls
                            if datetime.strptime(pr['updated_at'],
                                                 '%Y-%m-%dT%H:%M:%SZ') > cutoff_date]
            pulls.extend(recent_pulls)
            # If we got fewer PRs than requested, we've hit the end
            # Or if the last PR is older than our cutoff, we can stop
            if len(page_pulls) < 100 or \
               datetime.strptime(page_pulls[-1]['updated_at'], '%Y-%m-%dT%H:%M:%SZ') <= cutoff_date:
                break
            page += 1
        return pulls

    def has_recent_activity(self, repo_name):
        """Check if repository has had any activity in the last 7 days"""
        response = self.call_github_api(f"repos/{self.org_name}/{repo_name}/events",
                                        params={'per_page': 1})
        if response.status_code != 200:
            return True  # If we can't check, assume there's activity to be safe
        events = response.json()
        if not events:
            return False
        latest = datetime.strptime(events[0]['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        cutoff_date = self.get_cutoff_date()
        return latest > cutoff_date

    def get_repo_events(self, repo_name):
        """Get repository events for the last 7 days"""
        events = []
        page = 1
        cutoff_date = self.get_cutoff_date()
        while True:
            response = requests.get(f'{self.base_url}/repos/{self.org_name}/{repo_name}/events',
                                    headers=self.headers, params={'page': page, 'per_page': 100},
                                    timeout=10)
            if response.status_code != 200:
                return None
            page_events = response.json()
            if not page_events:
                break
            # Filter events in last 7 days
            recent_events = [
                event for event in page_events
                if datetime.strptime(event['created_at'], '%Y-%m-%dT%H:%M:%SZ') > cutoff_date
            ]
            events.extend(recent_events)
            # Stop if we've hit older events
            if len(page_events) < 100 or \
               datetime.strptime(page_events[-1]['created_at'],
                                 '%Y-%m-%dT%H:%M:%SZ') <= cutoff_date:
                break
            page += 1
        return events

    def process_repo(self, repo):
        """Process a single repository"""
        stats = {'total_stars': repo['stargazers_count'],
                 'total_forks': repo['forks_count'],
                 'total_issues': repo['open_issues_count'],
                 'new_stars': 0,
                 'new_forks': 0,
                 'new_issues': 0,
                 'language': repo['language'],
                 'pull_requests': {'total': 0, 'open': 0, 'closed': 0, 'merged': 0},
                 'merge_times': [],
                 'contributors': defaultdict(int)}
        if self.has_recent_activity(repo['name']):
            # Get events for stars, forks, and issues
            events = self.get_repo_events(repo['name'])
            if events:
                for event in events:
                    if event['type'] == 'WatchEvent':  # Star event
                        stats['new_stars'] += 1
                    elif event['type'] == 'ForkEvent':
                        stats['new_forks'] += 1
                    elif event['type'] == 'IssuesEvent' and event['payload']['action'] == 'opened':
                        stats['new_issues'] += 1
            # Get PR stats
            pulls = self.get_pull_stats(repo['name'])
            if pulls:
                for pr in pulls:
                    stats['pull_requests']['total'] += 1
                    stats['contributors'][pr['user']['login']] += 1
                    if pr['state'] == 'open':
                        stats['pull_requests']['open'] += 1
                    else:
                        stats['pull_requests']['closed'] += 1
                        if pr['merged_at']:
                            stats['pull_requests']['merged'] += 1
                            created = datetime.strptime(pr['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                            merged = datetime.strptime(pr['merged_at'], '%Y-%m-%dT%H:%M:%SZ')
                            merge_time = (merged - created).total_seconds() / 3600
                            stats['merge_times'].append(merge_time)
        return stats

    def generate_report(self):
        """Generate a comprehensive report of organization statistics"""
        # Initialize client for parallel processing
        client = Client()
        repos = self.get_repos()
        members = self.get_members()
        # Process repos in parallel
        bag = db.from_sequence(repos)
        results = bag.map(self.process_repo).compute()
        # Aggregate results
        stats = {'total_repos': len(repos),
                 'total_members': len(members),
                 'total_stars': 0,
                 'total_forks': 0,
                 'total_issues': 0,
                 'new_stars': 0,
                 'new_forks': 0,
                 'new_issues': 0,
                 'languages': defaultdict(int),
                 'pull_requests': {'total': 0, 'open': 0, 'closed': 0, 'merged': 0,
                                   'avg_time_to_merge': 0,
                                   'time_period': f'{self.days} days'},
                 'contributors': defaultdict(int)}
        merge_times = []
        for result in results:
            stats['total_stars'] += result['total_stars']
            stats['total_forks'] += result['total_forks']
            stats['total_issues'] += result['total_issues']
            stats['new_stars'] += result['new_stars']
            stats['new_forks'] += result['new_forks']
            stats['new_issues'] += result['new_issues']
            if result['language']:
                stats['languages'][result['language']] += 1
            stats['pull_requests']['total'] += result['pull_requests']['total']
            stats['pull_requests']['open'] += result['pull_requests']['open']
            stats['pull_requests']['closed'] += result['pull_requests']['closed']
            stats['pull_requests']['merged'] += result['pull_requests']['merged']
            merge_times.extend(result['merge_times'])
            for contributor, count in result['contributors'].items():
                stats['contributors'][contributor] += count
        if merge_times:
            stats['pull_requests']['avg_time_to_merge'] = sum(merge_times) / len(merge_times)
        client.close()
        return stats

def process():
    ''' Get and displayGitHub organization statistics
        Keyword arguments:
          None
        Returns:
          None
    '''
    try:
        stats = GitHubOrgStats(ARG.TOKEN, ARG.ORG)
        stats.days = ARG.DAYS
        report = stats.generate_report()
        # Print report
        print(f"\nStatistics for {ARG.ORG}")
        print("-" * 80)
        print(f"Total Repositories: {report['total_repos']:,}")
        print(f"Total Members:      {report['total_members']:,}")
        print(f"Total Stars:        {report['total_stars']:,}")
        print(f"Total Forks:        {report['total_forks']:,}")
        print(f"Total Issues:       {report['total_issues']:,}")

        print(f"\nActivity (last {ARG.DAYS} days):")
        print(f"New Stars:  {report['new_stars']:,}")
        print(f"New Forks:  {report['new_forks']:,}")
        print(f"New Issues: {report['new_issues']:,}")

        print(f"\nPull Request statistics (last {ARG.DAYS} days):")
        print(f"  Total PRs:  {report['pull_requests']['total']:,}")

        total = report['pull_requests']['total']
        if total > 0:
            open_pct = (report['pull_requests']['open'] / total) * 100
            closed_pct = (report['pull_requests']['closed'] / total) * 100
            merged_pct = (report['pull_requests']['merged'] / total) * 100
            print(f"  Opened PRs: {report['pull_requests']['open']:,} ({open_pct:.1f}%)")
            print(f"  Closed PRs: {report['pull_requests']['closed']:,} ({closed_pct:.1f}%)")
            print(f"  Merged PRs: {report['pull_requests']['merged']:,} ({merged_pct:.1f}%)")
        else:
            print("  Opened PRs: 0")
            print("  Closed PRs: 0")
            print("  Merged PRs: 0")
        if report['pull_requests']['avg_time_to_merge']:
            hours = report['pull_requests']['avg_time_to_merge']
            days = hours / 24
            print(f"  Average Time to Merge: {days:.1f} days ({hours:.1f} hours)")
        if report['contributors']:
            print(f"\nActive contributors (Last {ARG.DAYS} days):")
            for author, count in sorted(report['contributors'].items(),
                                     key=lambda x: (-x[1], x[0])):  # Sort by count desc, then name
                print(f"  {author}: {count} PR" + ("s" if count != 1 else ""))
        # I'm not convinced this is useful
        # print("\nLanguages used:")
        # for lang, count in sorted(report['languages'].items(), key=lambda x: x[1], reverse=True):
        #     print(f"  {lang}: {count} repositories")
    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GitHub Organization Statistics')
    parser.add_argument('--org', dest='ORG', default='JaneliaSciComp',
                        help='GitHub organization name')
    parser.add_argument('--token', dest='TOKEN', default=os.environ.get('GITHUB_TOKEN'),
                        help='GitHub API token')
    parser.add_argument('--days', dest='DAYS', type=int, default=7,
                        help='Number of days to look back (default: 7)')
    ARG = parser.parse_args()
    if not ARG.TOKEN:
        print("Error: GitHub token is required. Either pass in --token or set " \
              + "GITHUB_TOKEN environment variable.")
        sys.exit(-1)
    process()
