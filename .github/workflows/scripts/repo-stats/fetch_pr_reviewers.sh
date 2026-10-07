FROM_DATE=$1
TO_DATE=$2

gh api graphql --paginate \
  -f query='
query($searchQuery: String!, $endCursor: String) {
  search(query: $searchQuery, type: ISSUE, first: 100, after: $endCursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on PullRequest {
        number
        title
        createdAt
        merged
        mergedAt
        author {
          login
        }

        latestOpinionatedReviews(first: 100) {
          nodes {
            author {
              login
            }
            state
            submittedAt
            url
          }
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
  -f searchQuery="repo:world-federation-of-advertisers/cross-media-measurement is:pr merged:${FROM_DATE}..${TO_DATE}"
