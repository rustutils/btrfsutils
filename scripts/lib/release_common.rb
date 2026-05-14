# frozen_string_literal: true
#
# Shared release helpers — used by both scripts/release.rb (GitLab)
# and scripts/release_github.rb (GitHub mirror).

module ReleaseCommon
  CHANGELOG  = 'CHANGELOG.md'
  CARGO_TOML = 'Cargo.toml'

  # Returns [tag, version, prerelease?] from $CI_COMMIT_TAG.
  def self.read_tag
    tag = ENV.fetch('CI_COMMIT_TAG')
    version = tag.sub(/\Av/, '')
    [tag, version, version.include?('-')]
  end

  # Read [workspace.package].version from Cargo.toml. Cheap regex
  # walk — avoids pulling in a TOML gem for one field.
  def self.cargo_version
    in_section = false
    File.foreach(CARGO_TOML) do |line|
      if line.start_with?('[')
        in_section = (line.strip == '[workspace.package]')
        next
      end
      if in_section && line =~ /\Aversion\s*=\s*"([^"]+)"/
        return Regexp.last_match(1)
      end
    end
    nil
  end

  # Bail if the tag (sans v) doesn't match Cargo.toml exactly. We
  # always bump Cargo.toml for crates.io publishing, so prereleases
  # carry the suffix in both places.
  def self.check_cargo_version!(version)
    crate_version = cargo_version
    if crate_version.nil?
      warn "ERROR: no [workspace.package] version found in #{CARGO_TOML}"
      exit 1
    end
    return if crate_version == version

    warn "ERROR: tag #{version} does not match Cargo.toml version #{crate_version}"
    exit 1
  end

  # Pull a `## <heading>` or `## [<heading>]` section out of
  # CHANGELOG.md and return its body (everything until the next
  # `## ` heading), trimmed. Bracketed form is the keepachangelog
  # convention; unbracketed is what our CHANGELOG.md currently uses,
  # so we accept both and let CHANGELOG migration happen separately.
  def self.extract_section(heading)
    h = Regexp.escape(heading)
    matcher = /\A## (?:\[#{h}\]|#{h})(?:\s|\z)/
    in_block = false
    buf = []
    File.foreach(CHANGELOG) do |line|
      if line.start_with?('## ')
        break if in_block
        in_block = line.match?(matcher)
        next
      end
      buf << line if in_block
    end
    buf.join.strip
  end

  # CHANGELOG lookup with prerelease fallback.
  #   - Real release tag: require [X.Y.Z] block. Hard fail if missing.
  #   - Prerelease tag: try [X.Y.Z-suffix] first, fall back to [Unreleased].
  def self.release_notes(version, prerelease)
    notes = extract_section(version)
    notes = extract_section('Unreleased') if notes.empty? && prerelease
    if notes.empty?
      warn "ERROR: no CHANGELOG entry for [#{version}]"
      exit 1
    end
    notes
  end
end
