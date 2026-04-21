import { useCallback, useState } from "react";

export function usePageFeedback() {
  const [error, setError] = useState("");

  const clear = useCallback(() => {
    setError("");
  }, []);

  const showMessage = useCallback((_nextMessage: string) => {
  }, []);

  const showError = useCallback((nextError: string) => {
    setError(nextError);
  }, []);

  const setFeedback = useCallback((_nextMessage = "", nextError = "") => {
    setError(nextError);
  }, []);

  return {
    message: "",
    error,
    clear,
    showMessage,
    showError,
    setFeedback,
  };
}
