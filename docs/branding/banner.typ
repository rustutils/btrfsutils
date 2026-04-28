// btrfsutils banner — light/dark variants on a transparent background.
// Compile: typst compile banner.typ banner-dark.svg  --input theme=dark
//      or: typst compile banner.typ banner-light.svg --input theme=light
// (default theme is dark)

#import "@preview/cetz:0.5.0"

// ---- Theme switch -----------------------------------------------------------
#let theme = sys.inputs.at("theme", default: "dark")

// ---- Brand palette ----------------------------------------------------------
#let accent  = rgb("#d97757")
#let fg      = if theme == "light" { rgb("#1a1a1a") } else { rgb("#f5f1eb") }
#let muted   = if theme == "light" { rgb("#6a6055") } else { rgb("#a89888") }
#let divider = accent.transparentize(60%)

// ---- Page: 680x240 banner, no margins, transparent background -------------
#set page(width: 680pt, height: 240pt, margin: 0pt, fill: none)
#set text(font: "Inter", fill: fg)

#cetz.canvas(length: 1pt, {
  import cetz.draw: *

  // Native CeTZ coords: (0,0) is bottom-left. Banner is 680 wide, 240 tall.

  // Anchor the canvas bbox to the full page so coords are page-relative.
  hide(rect((0, 0), (680, 240)), bounds: true)

  // ---- Compact B-tree mark on the left ------------------------------------
  // The mark sits in a ~120pt-wide cluster centered around x=90.
  // Layout in y (banner-relative):
  //   root:      y = 175
  //   internals: y = 105
  //   leaves:    y = 50

  set-style(stroke: (paint: accent, thickness: 2.5pt, cap: "round"))

  // Edges: root -> 3 internals
  bezier((90, 162), (50, 118), (90, 140))
  line((90, 162), (90, 118))
  bezier((90, 162), (130, 118), (90, 140))

  // Edges: internals -> leaves
  line((50, 97),  (35, 55))
  line((50, 97),  (65, 55))
  line((90, 97),  (90, 55))
  line((130, 97), (115, 55))
  line((130, 97), (145, 55))

  // Root node: ring (drawn as thick stroke so the inner stays transparent) + center dot.
  // Ring spans r=6..11; stroke radius = 8.5, thickness = 5.
  circle((90, 175), radius: 8.5, stroke: (paint: accent, thickness: 5pt), fill: none)
  circle((90, 175), radius: 2.5, fill: accent, stroke: none)

  // Internal row: rings spanning r=4..8; stroke radius = 6, thickness = 4.
  for x in (50, 90, 130) {
    circle((x, 105), radius: 6, stroke: (paint: accent, thickness: 4pt), fill: none)
  }

  // Leaves: solid filled circles.
  for x in (35, 65, 90, 115, 145) {
    circle((x, 50), radius: 5, fill: accent, stroke: none)
  }

  // ---- Vertical divider ---------------------------------------------------
  line(
    (210, 70), (210, 170),
    stroke: (paint: divider, thickness: 1pt),
  )

  // ---- Wordmark + tagline -------------------------------------------------
  // CeTZ content() places typeset content at a given anchor point.
  // "west" = the left edge of the text box sits on the anchor.
  content(
    (240, 135),
    anchor: "west",
    text(size: 56pt, weight: 500, tracking: -1pt)[
      btrfs#text(fill: accent)[utils]
    ],
  )

  content(
    (240, 80),
    anchor: "west",
    text(font: "CommitMono", size: 14pt, fill: muted, tracking: 0.5pt)[
      btrfs userspace utilities, written in rust
    ],
  )
})
