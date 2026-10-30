import { Router } from 'express';

export const router = Router();

// Temporarily disabled: fetching user-specified URLs decompresses Brotli
// responses, which is exposed to CVE-2026-111497 until the Lambda Node.js
// runtime includes the fix (22.23.4+). Restore the getWebText handler once
// the runtime is confirmed updated.
router.get('/', (_req, res) => {
  res
    .status(503)
    .json({ message: 'Web content extraction is temporarily disabled.' });
});
