// ###################################################################################
// ###################################################################################
// ###################################################################################
// ###################################################################################
// ###################################################################################
// 1. Types creeeren: Props
// type Props = Weak<PropsUIPromptQuestionnaire> & ReactFactoryContext
// in types/prompts
// 2. API item toevoegen in port API 
// 3. testen
// 4. code splitten

import React, { useState } from 'react';
import { ReactFactoryContext } from '../../factory'
import { assert, Weak } from '../../../../helpers'
import { LabelButton, PrimaryButton } from '../elements/button'
import { PropsUIPromptQuestionnaire } from '../../../../types/prompts'

type Props = Weak<PropsUIPromptQuestionnaire> & ReactFactoryContext

export const Questionnaire = (props: Props): JSX.Element => {
  const [selectedChoice, setSelectedChoice] = useState<string | null>(null);
  const { question, choices, resolve } = props

  function handleDonate (): void {
    const value = "ASD"
    //const value = serializeConsentData()
    resolve?.({ __type__: 'PayloadJSON', value })
  }

  function handleCancel (): void {
    resolve?.({ __type__: 'PayloadFalse', value: false })
  }

  const handleChoiceSelect = (choice: string) => {
    setSelectedChoice(choice);
  };

  return (
    <div>
      <h3>{question}</h3>
      <ul>
        {choices.map((choice, index) => (
          <li key={index}>
            <label>
              <input
                type="radio"
                name="choice"
                value={choice}
                checked={selectedChoice === choice}
                onChange={() => handleChoiceSelect(choice)}
              />
              {choice}
            </label>
          </li>
        ))}
      </ul>
      {selectedChoice && <p>Selected choice: {selectedChoice}</p>}

      <div className='flex flex-row gap-4 mt-4 mb-4'>
        <PrimaryButton label="VERDER" onClick={handleDonate} color='bg-success text-white' />
        <LabelButton label="CANCEL" onClick={handleCancel} color='text-grey1' />
      </div>
    </div>
  );
};



//import { assert, Weak } from '../../../../helpers'
//import { PropsUITable, PropsUITableBody, PropsUITableCell, PropsUITableHead, PropsUITableRow } from '../../../../types/elements'
//import { PropsUIPromptConsentForm, PropsUIPromptConsentFormTable } from '../../../../types/prompts'
//import { Table } from '../elements/table'
//import { LabelButton, PrimaryButton } from '../elements/button'
//import { BodyLarge, Title4 } from '../elements/text'
//import TextBundle from '../../../../text_bundle'
//import { Translator } from '../../../../translator'
//import { ReactFactoryContext } from '../../factory'
//import React from 'react'
//import _ from 'lodash'
//
//type Props = Weak<PropsUIPromptConsentForm> & ReactFactoryContext
//
//interface TableContext {
//  title: string
//  deletedRowCount: number
//}
//
//export const ConsentForm = (props: Props): JSX.Element => {
//  const tablesIn = React.useRef<Array<PropsUITable & TableContext>>(parseTables(props.tables))
//  const metaTables = React.useRef<Array<PropsUITable & TableContext>>(parseTables(props.metaTables))
//  const tablesOut = React.useRef<Array<PropsUITable & TableContext>>(tablesIn.current)
//
//  const { locale, resolve } = props
//  const { description, donateQuestion, donateButton, cancelButton } = prepareCopy(props)
//
//  function rowCell (dataFrame: any, column: string, row: number): PropsUITableCell {
//    const text = String(dataFrame[column][`${row}`])
//    return { __type__: 'PropsUITableCell', text: text }
//  }
//
//  function headCell (dataFrame: any, column: string): PropsUITableCell {
//    return { __type__: 'PropsUITableCell', text: column }
//  }
//
//  function columnNames (dataFrame: any): string[] {
//    return Object.keys(dataFrame)
//  }
//
//  function columnCount (dataFrame: any): number {
//    return columnNames(dataFrame).length
//  }
//
//  function rowCount (dataFrame: any): number {
//    if (columnCount(dataFrame) === 0) {
//      return 0
//    } else {
//      const firstColumn = dataFrame[columnNames(dataFrame)[0]]
//      return Object.keys(firstColumn).length - 1
//    }
//  }
//
//  function rows (data: any): PropsUITableRow[] {
//    const result: PropsUITableRow[] = []
//    for (let row = 0; row <= rowCount(data); row++) {
//      const id = `${row}`
//      const cells = columnNames(data).map((column: string) => rowCell(data, column, row))
//      result.push({ __type__: 'PropsUITableRow', id, cells })
//    }
//    return result
//  }
//
//  function parseTables (tablesData: PropsUIPromptConsentFormTable[]): Array<PropsUITable & TableContext> {
//    console.log('parseTables')
//    return tablesData.map((table) => parseTable(table))
//  }
//
//  function parseTable (tableData: PropsUIPromptConsentFormTable): (PropsUITable & TableContext) {
//    const id = tableData.id
//    const title = Translator.translate(tableData.title, props.locale)
//    const deletedRowCount = 0
//    const dataFrame = JSON.parse(tableData.data_frame)
//    const headCells = columnNames(dataFrame).map((column: string) => headCell(dataFrame, column))
//    const head: PropsUITableHead = { __type__: 'PropsUITableHead', cells: headCells }
//    const body: PropsUITableBody = { __type__: 'PropsUITableBody', rows: rows(dataFrame) }
//
//    return { __type__: 'PropsUITable', id, head, body, title, deletedRowCount }
//  }
//
//  function renderTable (table: (Weak<PropsUITable> & TableContext), readOnly = false): JSX.Element {
//    return (
//      <div key={table.id} className='flex flex-col gap-4 mb-4'>
//        <Title4 text={table.title} margin='' />
//        <Table {...table} readOnly={readOnly} locale={locale} onChange={handleTableChange} />
//      </div>
//    )
//  }
//
//  function handleTableChange (id: string, rows: PropsUITableRow[]): void {
//    const tablesCopy = tablesOut.current.slice(0)
//    const index = tablesCopy.findIndex(table => table.id === id)
//    if (index > -1) {
//      const { title, head, body: oldBody, deletedRowCount: oldDeletedRowCount } = tablesCopy[index]
//      const body: PropsUITableBody = { __type__: 'PropsUITableBody', rows }
//      const deletedRowCount = oldDeletedRowCount + (oldBody.rows.length - rows.length)
//      tablesCopy[index] = { __type__: 'PropsUITable', id, head, body, title, deletedRowCount }
//    }
//    tablesOut.current = tablesCopy
//  }
//
//  function handleDonate (): void {
//    const value = serializeConsentData()
//    resolve?.({ __type__: 'PayloadJSON', value })
//  }
//
//  function handleCancel (): void {
//    resolve?.({ __type__: 'PayloadFalse', value: false })
//  }
//
//  function serializeConsentData (): string {
//    const array = serializeTables().concat(serializeMetaData())
//    return JSON.stringify(array)
//  }
//
//  function serializeMetaData (): any[] {
//    return serializeMetaTables().concat(serializeDeletedMetaData())
//  }
//
//  function serializeTables (): any[] {
//    return tablesOut.current.map((table) => serializeTable(table))
//  }
//
//  function serializeMetaTables (): any[] {
//    return metaTables.current.map((table) => serializeTable(table))
//  }
//
//  function serializeDeletedMetaData (): any {
//    const rawData = tablesOut.current
//      .filter(({ deletedRowCount }) => deletedRowCount > 0)
//      .map(({ id, deletedRowCount }) => `User deleted ${deletedRowCount} rows from table: ${id}`)
//
//    const data = JSON.stringify(rawData)
//    return { user_omissions: data }
//  }
//
//  function serializeTable ({ id, head, body: { rows } }: PropsUITable): any {
//    const data = rows.map((row) => serializeRow(row, head))
//    return { [id]: data }
//  }
//
//  function serializeRow (row: PropsUITableRow, head: PropsUITableHead): any {
//    assert(row.cells.length === head.cells.length, `Number of cells in row (${row.cells.length}) should be equals to number of cells in head (${head.cells.length})`)
//    const keys = head.cells.map((cell) => cell.text)
//    const values = row.cells.map((cell) => cell.text)
//    return _.fromPairs(_.zip(keys, values))
//  }
//
//  return (
//    <>
//      <BodyLarge text={description} />
//      <div className='flex flex-col gap-8'>
//        {tablesIn.current.map((table) => renderTable(table))}
//        <div>
//          <BodyLarge margin='' text={donateQuestion} />
//          <div className='flex flex-row gap-4 mt-4 mb-4'>
//            <PrimaryButton label={donateButton} onClick={handleDonate} color='bg-success text-white' />
//            <LabelButton label={cancelButton} onClick={handleCancel} color='text-grey1' />
//          </div>
//        </div>
//      </div>
//    </>
//  )
//}
//
