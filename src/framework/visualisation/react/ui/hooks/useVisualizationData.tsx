import { TableWithContext } from '../../../../types/elements';
import { VisualizationType, VisualizationData, ChartVisualizationData } from '../../../../types/visualizations';
import { useEffect, useState } from 'react';

type Status = 'loading' | 'success' | 'error';

export default function useVisualizationData(
  table: TableWithContext,
  visualization: VisualizationType
): [VisualizationData | undefined, Status] {
  const [visualizationData, setVisualizationData] = useState<VisualizationData>();
  const [status, setStatus] = useState<Status>('loading');
  const [worker, setWorker] = useState<Worker>();

  useEffect(() => {
    const worker = new Worker(new URL('../workers/visualizationDataWorker.ts', import.meta.url));
    setWorker(worker);
    return () => {
      worker.terminate();
    };
  }, []);

  useEffect(() => {
    if (worker != null && window.Worker !== undefined) {
      setStatus('loading');
      worker.onmessage = (
        e: MessageEvent<{ status: Status; visualizationData: VisualizationData | undefined }>
      ) => {
        console.log('Worker message received:', e.data);

        // Check if the worker encountered an error
        if (e.data.status === 'error' || !e.data.visualizationData) {
          setStatus('error');
          setVisualizationData(undefined);
          return;
        }

        // Proceed if there is valid visualization data
        if ('data' in e.data.visualizationData && Array.isArray(e.data.visualizationData.data)) {

          // Filter out entries where '__x' is null, undefined, or 'NaN-QNaN'
          const cleanedData = (e.data.visualizationData as ChartVisualizationData).data.filter(
            (item: Record<string, any>) => item.__x !== null && item.__x !== undefined && item.__x !== 'NaN-QNaN' && item.__x !== 'NaN-Invalid Date' && item.__x !== 'Invalid Date' && item.__x !== 'NaN' && item.__x !== 'NaN-Invalid Date-NaN' && item.__x !== 'Invalid Date-NaN-NaN' && item.__x !== 'Invalid Date-NaN'
          );


          // If no valid data remains after filtering, set visualizationData to undefined
          if (cleanedData.length === 0) {
            setStatus('success'); // Set status as success because it's not an error, just no data to display
            setVisualizationData(undefined);
          } else {
            // Update the state with filtered data
            setVisualizationData({ 
              ...(e.data.visualizationData as ChartVisualizationData), 
              data: cleanedData 
            });
            setStatus('success');
          }
        } else {
          // If it's not ChartVisualizationData, just set it as it is
          setVisualizationData(e.data.visualizationData);
          setStatus('success');
        }
      };

      worker.onerror = (error) => {
        console.warn('Worker error caught:', error);
        setStatus('error');
        setVisualizationData(undefined);
      };

      worker.postMessage({ table, visualization });
    }
  }, [table, visualization, worker]);

  return [visualizationData, status];
}
