import { PropsUITable, TableContext, TableWithContext } from '../../../../types/elements';
import {
  ChartVisualization,
  TextVisualization,
  VisualizationType,
  VisualizationData
} from '../../../../types/visualizations';
import { prepareChartData } from './visualizationDataFunctions/prepareChartData';
import { prepareTextData } from './visualizationDataFunctions/prepareTextData';

interface Input {
  table: TableWithContext;
  visualization: VisualizationType;
}

self.onmessage = (e: MessageEvent<Input>) => {
  createVisualizationData(e.data.table, e.data.visualization)
    .then((visualizationData) => {
      self.postMessage({ status: 'success', visualizationData });
    })
    .catch((error) => {
      // Specific handling for RangeError
      if (error instanceof RangeError && error.message === 'Invalid time value') {
        console.info('');
      } else {
        console.error('Error in visualizationDataWorker:', error);
      }
      self.postMessage({ status: 'error', visualizationData: undefined });
    });
}

async function createVisualizationData(
  table: PropsUITable & TableContext,
  visualization: VisualizationType
): Promise<VisualizationData> {
  try {
    if (table === undefined || visualization === undefined) {
      throw new Error('Table and visualization are required');
    }

    if (['line', 'bar', 'area'].includes(visualization.type)) {
      return await prepareChartData(table, visualization as ChartVisualization);
    }

    if (['wordcloud'].includes(visualization.type)) {
      return await prepareTextData(table, visualization as TextVisualization);
    }

    throw new Error(`Visualization type ${visualization.type} not supported`);
  } catch (error) {
    // Specific handling for RangeError
    if (error instanceof RangeError && error.message === 'Invalid time value') {
      console.info('Info: Invalid time value encountered in createVisualizationData:', error);
    } else {
      console.error('Error in createVisualizationData:', error);
    }
    throw error; // Re-throw the error to be caught by the .catch block in onmessage
  }
}
