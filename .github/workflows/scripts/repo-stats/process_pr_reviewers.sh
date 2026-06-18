#!/usr/bin/env python3

import os, json
from collections import defaultdict

pr_data = json.loads(os.environ["JSON"])

pr_authors_by_reviewer = defaultdict(lambda: defaultdict(int))

print("-------------------")
print("Total merged PRs")
print("-------------------")
print(len(pr_data))

for pr in pr_data:
  pr_author = pr.get("author").get("login")
  for review in pr.get("latestOpinionatedReviews").get("nodes"):
    review_author = review.get("author").get("login")
    pr_authors_by_reviewer[review_author][pr_author] +=1

print()
print("-------------------")
print("PRs reviewed")
print("-------------------")
for reviewer,authors_data in sorted(pr_authors_by_reviewer.items()):
  print(f"{reviewer} reviewed {sum(authors_data.values())} PRs authored by:")
  for author,count in sorted(authors_data.items(), key=lambda x: x[1], reverse=True):
    print(f"  {count} - {author}")
  print()