---
description: Code review a pull request
argument-hint: "[pull-request] [--comment]"
allowed-tools: Bash(gh pr comment:*), Bash(gh pr diff:*), Bash(gh pr view:*), mcp__github_inline_comment__create_inline_comment
# adapted from https://github.com/anthropics/claude-code/blob/main/plugins/code-review/commands/code-review.md
---

Provide a code review for the given pull request: $ARGUMENTS.

**Agent assumptions (applies to all agents and subagents):**
- All tools are functional and will work without error. Do not test tools or make exploratory calls. Make sure this is clear to every subagent that is launched.
- Only call a tool if it is required to complete the task. Every tool call should have a clear purpose.
- Use exactly one subagent. Do not specify a model override for it; it must inherit the driver's configured model.

To do this, follow these steps precisely:

1. Read CLAUDE.md first

2. View the pull request and obtain its title, description, and diff. Summarize the changes for your own context.

3. Launch one reviewer agent to independently review the changes. Give it the PR title and description, and ask it to return a list of issues where each issue includes a description and the reason it was flagged (e.g. "CLAUDE.md adherence", "bug"). The reviewer should:

   - Audit changes for compliance with applicable CLAUDE.md files. When evaluating a file, only consider CLAUDE.md files that share its directory or a parent directory.
   - Scan for obvious bugs in the diff, including security issues and incorrect logic.
   - Focus only on the changed code and the minimum context needed to validate it. Ignore nitpicks and likely false positives.

   **CRITICAL: We only want HIGH SIGNAL issues.** Flag issues where:
   - The code will fail to compile or parse (syntax errors, type errors, missing imports, unresolved references)
   - The code will definitely produce wrong results regardless of inputs (clear logic errors)
   - Clear, unambiguous CLAUDE.md violations where you can quote the exact rule being broken
   - Code Style violations

   Do NOT flag:
   - Potential issues that depend on specific inputs or state
   - Subjective suggestions or improvements

   If you are not certain an issue is real, do not flag it. False positives erode trust and waste reviewer time.

4. Validate each issue yourself against the diff and only the context needed to establish that it is real. Do not launch additional subagents for validation.

5. Filter out unvalidated or duplicate issues. This step will give us our list of high signal issues for our review.

6. Output a summary of the review findings to the terminal:
   - If issues were found, list each issue with a brief description.
   - If no issues were found, state: "No issues found. Checked for bugs and CLAUDE.md compliance."

   If `--comment` argument was NOT provided, stop here. Do not post any GitHub comments.

   If `--comment` argument IS provided and NO issues were found, post a summary comment using `gh pr comment` and stop.

   If `--comment` argument IS provided and issues were found, continue to step 7.

7. Create a list of all comments that you plan on leaving. This is only for you to make sure you are comfortable with the comments. Do not post this list anywhere.

8. Post inline comments for each issue using `mcp__github_inline_comment__create_inline_comment` with `confirmed: true`. For each comment:
   - Provide a brief description of the issue
   - For small, self-contained fixes, include a committable suggestion block
   - For larger fixes (6+ lines, structural changes, or changes spanning multiple locations), describe the issue and suggested fix without a suggestion block
   - Never post a committable suggestion UNLESS committing the suggestion fixes the issue entirely. If follow up steps are required, do not leave a committable suggestion.

   **IMPORTANT: Only post ONE comment per unique issue. Do not post duplicate comments.**

Use this list when evaluating issues in Steps 4 and 5 (these are false positives, do NOT flag):

- Pre-existing issues
- Something that appears to be a bug but is actually correct
- Pedantic nitpicks that a senior engineer would not flag
- Issues that a linter will catch (do not run the linter to verify)
- General code quality concerns (e.g., lack of test coverage, general security issues) unless explicitly required in CLAUDE.md
- Issues mentioned in CLAUDE.md but explicitly silenced in the code (e.g., via a lint ignore comment)

Notes:

- Use gh CLI to interact with GitHub (e.g., fetch pull requests, create comments). Do not use web fetch.
- Create a todo list before starting.
- You must cite and link each issue in inline comments (e.g., if referring to a CLAUDE.md, include a link to it).
- If no issues are found and `--comment` argument is provided, post a comment with the following format:

---

## Code review

No issues found. Checked for bugs and CLAUDE.md compliance.

---

- When linking to code in inline comments, follow the following format precisely, otherwise the Markdown preview won't render correctly: https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c21d3c10bc8e898b7ac1a2d745bdc9bc4e423afe/README.md#L10-L15
  - Requires full git sha
  - You must provide the full sha. Commands like `https://github.com/owner/repo/blob/$(git rev-parse HEAD)/foo/bar` will not work, since your comment will be directly rendered in Markdown.
  - Repo name must match the repo you're code reviewing
  - # sign after the file name
  - Line range format is L[start]-L[end]
  - Provide at least 1 line of context before and after, centered on the line you are commenting about (eg. if you are commenting about lines 5-6, you should link to `L4-7`)
