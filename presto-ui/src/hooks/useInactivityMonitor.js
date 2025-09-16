import { useEffect, useRef, useState } from 'react';

const useInactivityMonitor = ({
  timeout = 5000,
} = {}) => {
  const [isInactive, setIsInactive] = useState(false);
  const timerRef = useRef(null);
  const lastActivityRef = useRef(Date.now());

  const handleInactivity = () => {
    setIsInactive(true);
    console.log('User inactive, redirecting to logout page...'); // For testing
    window.location.href = 'logout.html';
    if (onInactive) {
      onInactive();
    }
  };

  const resetTimer = () => {
    lastActivityRef.current = Date.now();
    setIsInactive(false);
    if (timerRef.current) {
      clearTimeout(timerRef.current);
    }
    timerRef.current = setTimeout(() => {
      handleInactivity();
    }, timeout);
  };

  const handleActivity = () => {
    resetTimer();
  };

  const handleVisibilityChange = () => {
    const now = Date.now();
    const timeSinceLastActivity = now - lastActivityRef.current;

    if (timeSinceLastActivity >= timeout) {
      handleInactivity();
    } else {
      resetTimer();
    }
  };

  useEffect(() => {
    // Set up event listeners
    const events = ['mousemove', 'keydown', 'click', 'scroll'];
    events.forEach(event => {
      document.addEventListener(event, handleActivity, true);
    });

    // We need a dedicated handler for tab visibility changes
    document.addEventListener('visibilitychange', handleVisibilityChange);

    // Start the timer
    resetTimer();

    // Cleanup
    return () => {
      events.forEach(event => {
        document.removeEventListener(event, handleActivity, true);
      });
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      if (timerRef.current) {
        clearTimeout(timerRef.current);
      }
    };
  }, [timeout]);

  return { isInactive };
};

export default useInactivityMonitor;
