import { describe, expect, it } from 'vitest';
import { sanitizeExternalUrl } from './urlSecurity';

describe('sanitizeExternalUrl', () => {
  it('allows https URLs on allowed hosts', () => {
    const value = sanitizeExternalUrl('https://portal.azure.com/#resource/foo', {
      allowedHosts: ['portal.azure.com']
    });
    expect(value).toContain('https://portal.azure.com');
  });

  it('blocks javascript urls', () => {
    const value = sanitizeExternalUrl('javascript:alert(1)', {
      allowedHosts: ['portal.azure.com']
    });
    expect(value).toBe('');
  });

  it('blocks unapproved hosts', () => {
    const value = sanitizeExternalUrl('https://evil.example.com/path', {
      allowedHosts: ['portal.azure.com']
    });
    expect(value).toBe('');
  });

  it('supports wildcard hosts', () => {
    const value = sanitizeExternalUrl('https://sub.portal.azure.com/path', {
      allowedHosts: ['*.portal.azure.com']
    });
    expect(value).toContain('https://sub.portal.azure.com');
  });
});
