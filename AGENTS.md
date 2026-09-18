# Atlas Agent Rules

Read first:
- `CURRENT_STATUS.md`
- `README.md`
- `docs/`

Rules:
- Treat Atlas as a product repo, separate from the public Altira site.
- Use repo docs for current product truth before broader planning memos.
- Keep detailed conventions in repo docs, not in this file.

Preserve the no-synthetic-data posture and existing source, identity, and billing boundaries in the repository docs.

## Codex execution contract

Codex owns authorized work through implementation, verification, GitHub mechanics, and delivery where those stages are in scope. Claude is advisory unless Ryan explicitly requests a review. Complete ordinary implementation choices without introducing another approval stage.

- Start from the current default branch and verify the checkout before editing. Use repository docs for adopted product scope and observed runtime for deployment claims.
- Respect review-only, proof-only, draft-only, and no-deploy requests. Preserve explicit product, source, launch, and data holds; a settings or documentation change does not authorize their activation.
- Ask Ryan for genuine product or commercial judgment, privacy or licensing changes, credentials, destructive actions, or unresolved ownership conflicts. Do not ask again for a step already authorized.
- Read the final diff and run verification appropriate to the change, including required repository gates. Report what changed, the result, and material limitations; distinguish implemented, tested, merged, deployed, and live-verified.
- Preserve active and dirty worktrees. Use an isolated branch/worktree when needed; keep cleanup separate. Confirm a PR is merged before deleting its branch or worktree.
- When deployment is in scope, verify the relevant CI/deploy result and the live surface before reporting success. A merged documentation change alone does not require a deployment.
- Keep writes and migrations proportional, preserve migration ordering against the default branch, and stage intended paths explicitly. Never commit secrets or symlinked dependency directories.
- Use Altira and Records in new customer-facing language. Preserve compatible technical identifiers, source-backed claims, and the product's documented evidence and data boundaries.

## Write-back rule

Update `CURRENT_STATUS.md` only when settled product/runtime state or a lasting operating boundary changes. Keep it concise and preserve existing facts unless current evidence changes them. A routine instruction edit does not require rewriting product status.

Record a material completion once in the established completion log when available, with a short pointer to its PR or verification evidence. Keep detailed evidence with the work it supports. Do not create duplicate ledgers, log routine reads, or add a mandatory bookkeeping footer to the final answer. Do not revive deprecated handoff files.
