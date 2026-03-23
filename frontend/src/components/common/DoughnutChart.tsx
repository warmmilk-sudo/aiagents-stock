import { ArcElement, Chart as ChartJS, Legend, Tooltip, type ChartData, type ChartOptions } from "chart.js";
import { Doughnut } from "react-chartjs-2";

ChartJS.register(ArcElement, Legend, Tooltip);

interface DoughnutChartProps {
  data: ChartData<"doughnut", number[], string>;
  options?: ChartOptions<"doughnut">;
}

export function DoughnutChart({ data, options }: DoughnutChartProps) {
  return <Doughnut data={data} options={options} />;
}
