import { useConfigStore } from "../stores/configStore";

export function useSelectedModels() {
  const lightweightModel = useConfigStore((state) => state.fields.LIGHTWEIGHT_MODEL_NAME?.value ?? "");
  const reasoningModel = useConfigStore((state) => state.fields.REASONING_MODEL_NAME?.value ?? "");

  return {
    lightweightModel,
    reasoningModel,
  };
}
