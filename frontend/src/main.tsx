import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

// Prevent unhandled errors from crashing/navigating the page
window.addEventListener('error', (e) => {
  console.error('[KafkaDebugFlow] Unhandled error:', e.error);
  e.preventDefault();
});
window.addEventListener('unhandledrejection', (e) => {
  console.error('[KafkaDebugFlow] Unhandled promise rejection:', e.reason);
  e.preventDefault();
});

// Prevent accidental navigation away from SPA
window.addEventListener('click', (e) => {
  const target = e.target as HTMLElement;
  const anchor = target.closest('a');
  if (anchor && anchor.href && !anchor.href.startsWith('javascript:')) {
    const url = new URL(anchor.href, window.location.origin);
    if (url.origin === window.location.origin && url.pathname.startsWith('/api/')) {
      e.preventDefault();
      console.warn('[KafkaDebugFlow] Blocked navigation to API URL:', anchor.href);
    }
  }
}, true);

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
