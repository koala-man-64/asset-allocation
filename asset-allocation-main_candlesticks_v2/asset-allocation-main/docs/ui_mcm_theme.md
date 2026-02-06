# MCM Design System (Warm Geometric Minimalism)

## Goal
Apply a Mid-Century Modern (MCM) design language with warm neutrals, bold geometry, and high-contrast structure to the fin-detective UI.

Evidence: `ui/src/styles/theme.css`, `ui/src/styles/fonts.css`

## Core Tokens
### Color Palette
Base tokens are defined as CSS variables and mapped into Tailwind theme colors.

- `--mcm-cream` (#f5f5dc): primary background
- `--mcm-paper` (#fcf5e5): card surfaces
- `--mcm-walnut` (#773f1a): primary text + 2px borders
- `--mcm-teal` (#008080): secondary text + focus ring
- `--mcm-mustard` (#e1ad01): primary accent
- `--mcm-olive` (#808000): muted text accents

Evidence: `ui/src/styles/theme.css`

### Typography
- Display: Josefin Sans (headers, labels, buttons)
- Body: Montserrat (data + descriptions)

Evidence: `ui/src/styles/fonts.css`, `ui/src/styles/theme.css`

### Radius + Shadows
- Base radius: 1.5rem
- Card shadow: `8px 8px 0 rgba(119,63,26,0.1)`

Evidence: `ui/src/styles/theme.css`, `ui/src/app/components/ui/card.tsx`

## Component Rules
### Cards
- 2px walnut border
- Rounded 1.6rem
- Offset shadow and abstract circle accents

Evidence: `ui/src/app/components/ui/card.tsx`

### Buttons
- Primary: mustard background, walnut text, 2px border, rounded-full
- Secondary: cream background, walnut text, 2px border, rounded-full
- Motion: hover scale 1.05, active scale 0.95 (reduced motion safe)

Evidence: `ui/src/app/components/ui/button.tsx`, `ui/src/styles/theme.css`

### Form Elements
- Inputs/selects/textarea: cream background, 2px walnut border, rounded-xl
- Labels: 10px uppercase, tracking-widest

Evidence: `ui/src/app/components/ui/input.tsx`, `ui/src/app/components/ui/select.tsx`, `ui/src/app/components/ui/textarea.tsx`, `ui/src/app/components/ui/label.tsx`

### Tables
- `border-separate` with row spacing
- Rows rendered as floating islands with 2px walnut borders
- Headings: uppercase micro-labels

Evidence: `ui/src/app/components/ui/table.tsx`, `ui/src/app/components/common/DataTable.tsx`

### Data Quality Page
- Updated palette + geometry to match MCM system while preserving layout

Evidence: `ui/src/styles/data-quality.css`

## Motion
- Global page-load rise animation applied to `#root`
- Reduced motion supported via media query

Evidence: `ui/src/styles/theme.css`

## Usage Guidance
- Use `bg-mcm-cream` for main surfaces and `bg-mcm-paper` for cards.
- Use `text-mcm-walnut` for primary text; `text-mcm-olive` for subdued labels.
- Use `lowercase` for data text where appropriate to contrast with uppercase labels.

Evidence: `ui/src/styles/theme.css`, `ui/src/app/components/common/DataTable.tsx`
