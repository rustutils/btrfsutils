#!/usr/bin/env ruby
# frozen_string_literal: true
#
# Mirrors the GitLab release to GitHub. Uses gh CLI to create a
# release on the github.com mirror at the same path as the GitLab
# project (we maintain rustutils/<project> on both). Invoked from
# .gitlab-ci.yml's `release-github` job.
#
# Auth: GITHUB_TOKEN env var (rustutils-org-scoped fine-grained PAT,
# Contents: read+write). gh CLI picks this up automatically.
#
# Local dry-run:
#   CI_COMMIT_TAG=v0.13.0 \
#   CI_PROJECT_PATH=rustutils/btrfsutils \
#   GITHUB_TOKEN=... \
#   ruby scripts/release_github.rb --print

require_relative 'lib/release_common'

DIST_DIR = 'target/dist'
NOTES_FILE = 'release-notes.md'

print_only = ARGV.include?('--print')

tag, version, prerelease = ReleaseCommon.read_tag
repo = ENV.fetch('CI_PROJECT_PATH')

ReleaseCommon.check_cargo_version!(version)
notes = ReleaseCommon.release_notes(version, prerelease)

# gh wants a notes file rather than a long --notes string — sidesteps
# any shell-quoting concerns when the CHANGELOG body has backticks /
# pipes / quotes. Written to repo root; the runner is ephemeral so no
# cleanup needed.
File.write(NOTES_FILE, notes) unless print_only

artifacts = Dir["#{DIST_DIR}/*"].sort

cmd = [
  'gh', 'release', 'create', tag,
  '--repo', repo,
  '--title', "Release #{tag}",
  '--notes-file', NOTES_FILE,
  *(prerelease ? ['--prerelease'] : []),
  *artifacts,
]

if print_only
  puts cmd.inspect
else
  exec(*cmd)
end
