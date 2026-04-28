// btrfsutils logo
// Compile: typst compile logo.typ logo.svg
//      or: typst compile logo.typ logo.png --ppi 300

#import "@preview/cetz:0.5.0"

// ---- Brand palette ----------------------------------------------------------
#let bg     = rgb("#1a1a1a")
#let accent = rgb("#d97757")  // Rust-leaning orange

// ---- Page: square, no margins, dark background ----------------------------
#set page(width: 680pt, height: 680pt, margin: 0pt, fill: bg)

#cetz.canvas(length: 1pt, {
  import cetz.draw: *

  // Native CeTZ coords: (0,0) is bottom-left, y increases upward.
  // Tree is drawn top-down: root at high y, leaves at low y.
  //
  //   Layer 1 (root):      y = 500
  //   Layer 2 (internals): y = 310
  //   Layer 3 (leaves):    y = 170

  // Anchor the canvas bbox to the full page so coords are page-relative.
  hide(rect((0, 0), (680, 680)), bounds: true)

  // Edges first, so nodes paint over the edge endpoints.
  set-style(stroke: (paint: accent, thickness: 6pt, cap: "round"))

  // Root -> 3 internals (the outer two curve outward)
  bezier((340, 480), (200, 320), (340, 400))
  line((340, 480), (340, 320))
  bezier((340, 480), (480, 320), (340, 400))

  // Left internal -> 2 leaves
  bezier((200, 290), (140, 170), (200, 230))
  line((200, 290), (200, 170))

  // Center internal -> 2 leaves
  line((340, 290), (280, 170))
  line((340, 290), (400, 170))

  // Right internal -> 2 leaves
  bezier((480, 290), (440, 170), (480, 230))
  line((480, 290), (540, 170))

  // ---- Helper: ringed node (accent ring with bg-colored hole) -------------
  let ringed-node(pos, r-outer, r-inner, has-dot: false) = {
    circle(pos, radius: r-outer, fill: accent, stroke: none)
    circle(pos, radius: r-inner, fill: bg,     stroke: none)
    if has-dot {
      circle(pos, radius: r-inner * 0.36, fill: accent, stroke: none)
    }
  }

  // Root node (slightly larger, with center dot)
  ringed-node((340, 500), 36, 22, has-dot: true)

  // Internal row
  ringed-node((200, 310), 28, 16)
  ringed-node((340, 310), 28, 16)
  ringed-node((480, 310), 28, 16)

  // Leaf row: solid filled circles
  for x in (140, 200, 280, 400, 440, 540) {
    circle((x, 170), radius: 20, fill: accent, stroke: none)
  }
})
