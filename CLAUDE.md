# CLAUDE.md — fork-local instructions

This file exists only on this fork (acseven). It must NEVER be committed to a
branch intended for upstream (hoxxep/UNAS-Pro-fan-control) or included in any PR.

## Hard rules

- **NEVER open, create, or push a pull request to upstream (hoxxep) without the
  user's explicit consent in the current session.** No exceptions, no implied
  approval from earlier conversations.
- **The user tests on real hardware first.** Any change touching fan control,
  sensors, or MQTT must be tested by the user on their UNAS device before a PR
  is even proposed. Do not treat passing local checks as a substitute.
- Pushing to this fork's branches is fine; opening PRs against this fork is fine
  when asked. The upstream PR is the only gated action.
- **Touch the original repo's files at a minimum.** New functionality goes in
  new files. Existing files (fan_control.sh, fan_control.service, deploy.sh,
  README.md, ...) may only receive the smallest hooks/diffs needed to wire the
  new files in.

## Workflow

1. Implement on a feature branch on this fork.
2. User deploys and tests on the device (`./deploy.sh $HOST`, `./query.sh $HOST`).
3. Only after the user confirms the test AND explicitly says to PR upstream:
   prepare the PR — excluding this CLAUDE.md.
