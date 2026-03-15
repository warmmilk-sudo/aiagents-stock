import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

interface UseDetailRouteStateOptions {
  idParam?: string;
  viewParam?: string;
  detailValue?: string;
}

export function useDetailRouteState(options: UseDetailRouteStateOptions = {}) {
  const {
    idParam = "recordId",
    viewParam = "view",
    detailValue = "detail",
  } = options;
  const [searchParams, setSearchParams] = useSearchParams();

  const detailId = useMemo(
    () => Number(searchParams.get(idParam) || 0),
    [idParam, searchParams],
  );
  const isDetail = searchParams.get(viewParam) === detailValue || detailId > 0;

  const openDetail = (id: number, extraParams?: Record<string, string>) => {
    const nextParams = new URLSearchParams(searchParams);
    nextParams.set(viewParam, detailValue);
    nextParams.set(idParam, String(id));
    Object.entries(extraParams ?? {}).forEach(([key, value]) => nextParams.set(key, value));
    setSearchParams(nextParams);
  };

  const closeDetail = (keysToPreserve: string[] = []) => {
    const nextParams = new URLSearchParams();
    keysToPreserve.forEach((key) => {
      const value = searchParams.get(key);
      if (value) {
        nextParams.set(key, value);
      }
    });
    setSearchParams(nextParams);
  };

  return {
    isDetail,
    detailId,
    searchParams,
    openDetail,
    closeDetail,
  };
}
