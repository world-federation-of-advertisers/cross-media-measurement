EVENT=$1 # closed | created | etc.
FROM_DATE=$2
TO_DATE=$3

gh api graphql --paginate \
  -f query='
query($searchQuery: String!, $endCursor: String) {
  search(query: $searchQuery, type: ISSUE, first: 100, after: $endCursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on Issue {
        number
        title
        issueType {
          name
        }
        createdAt
        closed
        closedAt
        state
        stateReason
        author {
          login
        }
      }
    }
  }
  rateLimit {
    cost
    used
    remaining
    limit
    resetAt
    nodeCount
  }
}
' \
  -f searchQuery="repo:world-federation-of-advertisers/cross-media-measurement is:issue ${EVENT}:${FROM_DATE}..${TO_DATE}"