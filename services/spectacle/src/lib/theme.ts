/**
 * Standardized typography and theme constants for Spectacle
 */

export const fonts = {
  /** Primary font family for body text and UI elements */
  primary: 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
  /** Monospace font family for code, data, and identifiers */
  mono: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
} as const;

export const fontSizes = {
  /** 12px - tiny labels, badges */
  xs: '0.75rem',
  /** 14px - small text, secondary content */
  sm: '0.875rem',
  /** 16px - base body text */
  base: '1rem',
  /** 18px - slightly larger text */
  lg: '1.125rem',
  /** 20px - section headings */
  xl: '1.25rem',
  /** 24px - major section titles */
  '2xl': '1.5rem',
  /** 32px - page headings */
  '3xl': '2rem',
  /** 40px - display/hero text */
  '4xl': '2.5rem',
} as const;

export const fontWeights = {
  /** 400 - normal body text */
  normal: '400',
  /** 500 - medium emphasis */
  medium: '500',
  /** 600 - section headings, labels */
  semibold: '600',
  /** 700 - strong emphasis, major headings */
  bold: '700',
} as const;

export const lineHeights = {
  /** Tight line height for headings */
  tight: '1.25',
  /** Normal line height for body text */
  normal: '1.5',
  /** Relaxed line height for readable blocks */
  relaxed: '1.75',
} as const;

export const colors = {
  text: {
    /** Primary text color */
    primary: '#1f2937',
    /** Secondary text color */
    secondary: '#374151',
    /** Tertiary/muted text color */
    muted: '#6b7280',
    /** Disabled/placeholder text */
    disabled: '#9ca3af',
  },
  background: {
    /** Page background */
    page: '#f9fafb',
    /** Card/surface background */
    surface: '#ffffff',
    /** Subtle background for sections */
    subtle: '#f3f4f6',
  },
  border: {
    /** Default border color */
    default: '#e5e7eb',
    /** Stronger border for emphasis */
    strong: '#d1d5db',
  },
  accent: {
    /** Primary accent (blue) */
    primary: '#3b82f6',
    /** Success (green) */
    success: '#16a34a',
    /** Warning (yellow) */
    warning: '#ca8a04',
    /** Error (red) */
    error: '#dc2626',
  },
} as const;

/**
 * Pre-composed text styles for common use cases
 */
export const textStyles = {
  /** Display heading (2.5rem, bold) */
  display: {
    fontSize: fontSizes['4xl'],
    fontWeight: fontWeights.bold,
    lineHeight: lineHeights.tight,
    color: colors.text.primary,
  },
  /** Page heading (2rem, bold) */
  h1: {
    fontSize: fontSizes['3xl'],
    fontWeight: fontWeights.bold,
    lineHeight: lineHeights.tight,
    color: colors.text.primary,
  },
  /** Section heading (1.5rem, semibold) */
  h2: {
    fontSize: fontSizes['2xl'],
    fontWeight: fontWeights.semibold,
    lineHeight: lineHeights.tight,
    color: colors.text.primary,
  },
  /** Subsection heading (1.25rem, semibold) */
  h3: {
    fontSize: fontSizes.xl,
    fontWeight: fontWeights.semibold,
    lineHeight: lineHeights.tight,
    color: colors.text.primary,
  },
  /** Body text (1rem, normal) */
  body: {
    fontSize: fontSizes.base,
    fontWeight: fontWeights.normal,
    lineHeight: lineHeights.normal,
    color: colors.text.primary,
  },
  /** Small text (0.875rem) */
  small: {
    fontSize: fontSizes.sm,
    fontWeight: fontWeights.normal,
    lineHeight: lineHeights.normal,
    color: colors.text.secondary,
  },
  /** Label text (0.75rem, medium, uppercase) */
  label: {
    fontSize: fontSizes.xs,
    fontWeight: fontWeights.medium,
    lineHeight: lineHeights.normal,
    textTransform: 'uppercase' as const,
    letterSpacing: '0.05em',
    color: colors.text.muted,
  },
  /** Code/data text (monospace) */
  code: {
    fontFamily: fonts.mono,
    fontSize: fontSizes.sm,
    fontWeight: fontWeights.normal,
    lineHeight: lineHeights.normal,
    color: colors.text.secondary,
  },
  /** Muted helper text */
  muted: {
    fontSize: fontSizes.sm,
    fontWeight: fontWeights.normal,
    lineHeight: lineHeights.normal,
    color: colors.text.muted,
  },
} as const;
