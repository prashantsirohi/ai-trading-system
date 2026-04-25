/** Title-case a snake_case or whitespace-separated string. */
export function titleCase(value: string | null | undefined): string {
  if (!value) {
    return 'Unknown';
  }
  return value
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join(' ');
}
