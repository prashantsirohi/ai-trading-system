import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { money, pct, TrustBanner } from './pages';

describe('trade journal trust and decimal presentation', () => {
  it.each(['PARTIAL', 'PROVISIONAL', 'STALE'])('renders %s as a limited state', (status) => {
    render(<TrustBanner status={status} />);
    expect(screen.getByRole('status')).toHaveTextContent(/coverage limitations/i);
  });

  it.each(['BLOCKED', 'CONFLICT', 'UNTRUSTED', 'UNAVAILABLE'])('renders %s as blocked evidence', (status) => {
    render(<TrustBanner status={status} />);
    expect(screen.getByRole('alert')).toHaveTextContent(/no missing values are fabricated/i);
  });

  it('renders trusted evidence distinctly', () => {
    render(<TrustBanner status="TRUSTED" />);
    expect(screen.getByRole('status')).toHaveTextContent(/backed by trusted/i);
  });

  it('formats copies for display without changing authoritative strings', () => {
    const authoritative = '7892527.99000000';
    expect(money(authoritative)).toContain('78,92,527.99');
    expect(authoritative).toBe('7892527.99000000');
    expect(pct('0.12345678')).toBe('12.35%');
  });
});
