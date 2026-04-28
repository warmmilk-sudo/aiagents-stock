import { useEffect, useRef, type DependencyList } from "react";

interface UsePollingLoaderOptions {
  load: () => Promise<void> | void;
  intervalMs?: number | null;
  enabled?: boolean;
  immediate?: boolean;
  dependencies?: DependencyList;
}

export function usePollingLoader({
  load,
  intervalMs,
  enabled = true,
  immediate = true,
  dependencies = [],
}: UsePollingLoaderOptions) {
  const loadRef = useRef(load);

  useEffect(() => {
    loadRef.current = load;
  }, [load]);

  useEffect(() => {
    if (!enabled) {
      return undefined;
    }

    const run = () => {
      void Promise.resolve()
        .then(() => loadRef.current())
        .catch((error) => {
          console.error("[usePollingLoader] load failed", error);
        });
    };

    if (immediate) {
      run();
    }

    if (!intervalMs) {
      return undefined;
    }

    const timer = window.setInterval(run, intervalMs);
    return () => window.clearInterval(timer);
  }, [enabled, immediate, intervalMs, ...dependencies]);
}
