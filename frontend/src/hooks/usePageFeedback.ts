import { useEffect, useState } from "react";

const FEEDBACK_TIMEOUT_MS = 5000;

export function usePageFeedback() {
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    if (!message && !error) {
      return undefined;
    }

    const timer = window.setTimeout(() => {
      setMessage("");
      setError("");
    }, FEEDBACK_TIMEOUT_MS);

    return () => {
      window.clearTimeout(timer);
    };
  }, [message, error]);

  const clear = () => {
    setMessage("");
    setError("");
  };

  const showMessage = (nextMessage: string) => {
    setMessage(nextMessage);
    setError("");
  };

  const showError = (nextError: string) => {
    setError(nextError);
    setMessage("");
  };

  const setFeedback = (nextMessage = "", nextError = "") => {
    setMessage(nextMessage);
    setError(nextError);
  };

  return {
    message,
    error,
    clear,
    showMessage,
    showError,
    setFeedback,
  };
}
