import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { AuthProvider, LoginView } from './auth';

describe('session-only authentication', () => {
  it('uses a password field and never exposes a credential in the URL', () => {
    const storageWrite = vi.spyOn(Storage.prototype, 'setItem');
    render(<AuthProvider><LoginView /></AuthProvider>);
    const input = screen.getByLabelText('API credential');
    expect(input).toHaveAttribute('type', 'password');
    fireEvent.change(input, { target: { value: 'private-key' } });
    expect(window.location.href).not.toContain('private-key');
    expect(storageWrite).not.toHaveBeenCalled();
  });
});
