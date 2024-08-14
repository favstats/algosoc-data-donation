import json
import pandas as pd
from typing import Dict, Any, List, Optional, Literal
from datetime import datetime
import logging
import zipfile
import io
import port.api.props as props
from pathlib import Path  # Import the Path object from the pathlib module

logger = logging.getLogger(__name__)

def create_chart(type: Literal["bar", "line", "area"], 
                 nl_title: str, en_title: str, 
                 x: str, y: str = 'Count', 
                 x_label: Optional[str] = None, y_label: Optional[str] = None,
                 date_format: Optional[str] = None, aggregate: str = "sum", 
                 addZeroes: bool = True, group_by: Optional[str] = None, 
                 df: Optional[pd.DataFrame] = None):
    
    values = []
    if group_by and df is not None:
        for group in df[group_by].unique():
            group_data = df[df[group_by] == group]
            values.append(props.PropsUIChartValue(
                column=y,
                label=str(group),
                aggregate=aggregate,
                addZeroes=addZeroes
            ))
    else:
        values.append(props.PropsUIChartValue(
            column=y,
            label=y_label or y,
            aggregate=aggregate,
            addZeroes=addZeroes
        ))

    chart = props.PropsUIChartVisualization(
        title=props.Translatable({"en": en_title, "nl": nl_title}),
        type=type,
        group=props.PropsUIChartGroup(column=x, label=x_label, dateFormat=date_format),
        values=values
    )

    logger.debug("Chart configuration:\n%s", json.dumps(chart.toDict(), indent=2))

    return chart




def create_wordcloud(nl_title: str, en_title: str, column: str, 
                     tokenize: bool = False, 
                     value_column: Optional[str] = None, 
                     extract: Optional[Literal["url_domain"]] = None):
    return props.PropsUITextVisualization(title = props.Translatable({"en": en_title, "nl": nl_title}),
                                          type='wordcloud',
                                          text_column=column,
                                          value_column=value_column,
                                          tokenize=tokenize,
                                          extract=extract)
