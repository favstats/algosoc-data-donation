import { assert, Weak } from '../../../../helpers'
import {
  PropsUITable,
  PropsUITableBody,
  PropsUITableHead,
  PropsUITableRow,
  TableWithContext,
  TableContext
} from '../../../../types/elements'
import { PropsUIPromptConsentForm, PropsUIPromptConsentFormTable } from '../../../../types/prompts'
import { LabelButton, PrimaryButton } from '../elements/button'
import { BodyLarge } from '../elements/text'
import { FormattedBodyLarge } from '../elements/text'
import TextBundle from '../../../../text_bundle'
import { Translator } from '../../../../translator'
import { ReactFactoryContext } from '../../factory'
import { useCallback, useEffect, useState } from 'react'
import _ from 'lodash'

import useUnloadWarning from '../hooks/useUnloadWarning'

import { TableContainer } from '../elements/table_container'

type Props = Weak<PropsUIPromptConsentForm> & ReactFactoryContext & { dynamicList?: JSX.Element }

export const ConsentForm = (props: Props): JSX.Element => {
  useUnloadWarning()
  const [tables, setTables] = useState<TableWithContext[]>(() => parseTables(props.tables))
  const [metaTables, setMetaTables] = useState<TableWithContext[]>(() =>
    parseTables(props.metaTables)
  )
  const { locale, resolve } = props
  const { description, donateQuestion, donateButton, cancelButton } = prepareCopy(props)

  useEffect(() => {
    setTables(parseTables(props.tables))
    setMetaTables(parseTables(props.metaTables))
  }, [props.tables])

  const updateTable = useCallback((tableId: string, table: TableWithContext) => {
    setTables((tables) => {
      const index = tables.findIndex((table) => table.id === tableId)
      if (index === -1) return tables

      const newTables = [...tables]
      newTables[index] = table
      return newTables
    })
  }, [])

  return (
    <>

      <div className="max-w-3xl">
<FormattedBodyLarge>
  In de tabel hieronder staan de gegevens die uit uw bestand zijn gehaald. U kunt ze nu eerst bekijken en kiezen wat u wel of niet wilt delen. Verwijder gegevens door op het vakje links ernaast te klikken en op ‘Verwijderen’ te drukken.<br /><br />
  
 Met de zoekfunctie rechts bovenin de tabel kunt u gemakkelijk trefwoorden zoeken. Zoek bijvoorbeeld eens op een naam of website die u kent. U kunt ook categorieën bekijken met de 'filter'-knop links bovenin de tabel. Het is interessant om te zien hoeveel gegevens bedrijven van ons opslaan!<br /><br />
  
  Als u alles hebt gecontroleerd klikt u onderaan deze pagina op ‘Ja, Doneer’. Als u toch niet wilt doneren klikt u op ‘Nee’.
</FormattedBodyLarge>
        {/* Insert the dynamic list */}
        {props.dynamicList && (
          <div>
            {props.dynamicList}
          </div>
        )}
      </div>
      <div className="flex flex-col gap-16 w-full">
        <div className="grid gap-8 max-w-full">
          {tables.map((table) => {
            return (
              <TableContainer
                key={table.id}
                id={table.id}
                table={table}
                updateTable={updateTable}
                locale={locale}
              />
            )
          })}
        </div>
        <div>
          <BodyLarge margin="" text={donateQuestion} />

          <div className="flex flex-row gap-4 mt-4 mb-4">
            <PrimaryButton
              label={donateButton}
              onClick={handleDonate}
              color="bg-success text-white"
            />
            <LabelButton label={cancelButton} onClick={handleCancel} color="text-grey1" />
          </div>
        </div>
      </div>
    </>
  )

  function handleDonate(): void {
    const value = serializeConsentData()
    resolve?.({ __type__: 'PayloadJSON', value })
  }

  function handleCancel(): void {
    resolve?.({ __type__: 'PayloadFalse', value: false })
  }

  function serializeConsentData(): string {
    const array = serializeTables().concat(serializeMetaData())
    return JSON.stringify(array)
  }

  function serializeMetaData(): any[] {
    return serializeMetaTables().concat(serializeDeletedMetaData())
  }

  function serializeTables(): any[] {
    return tables.map((table) => serializeTable(table))
  }

  function serializeMetaTables(): any[] {
    return metaTables.map((table) => serializeTable(table))
  }

  function serializeDeletedMetaData(): any {
    const rawData = tables
      .filter(({ deletedRowCount }) => deletedRowCount > 0)
      .map(({ id, deletedRowCount }) => `User deleted ${deletedRowCount} rows from table: ${id}`)

    const data = JSON.stringify(rawData)
    return { user_omissions: data }
  }

  function serializeTable({ id, head, body: { rows } }: PropsUITable): any {
    const data = rows.map((row) => serializeRow(row, head))
    return { [id]: data }
  }

  function serializeRow(row: PropsUITableRow, head: PropsUITableHead): any {
    assert(
      row.cells.length === head.cells.length,
      `Number of cells in row (${row.cells.length}) should be equals to number of cells in head (${head.cells.length})`
    )
    const keys = head.cells.map((cell) => cell)
    const values = row.cells.map((cell) => cell)
    return _.fromPairs(_.zip(keys, values))
  }

  function rowCell(dataFrame: any, column: string, row: number): string {
    return String(dataFrame[column][`${row}`])
  }

  function columnNames(dataFrame: any): string[] {
    return Object.keys(dataFrame)
  }

  function columnCount(dataFrame: any): number {
    return columnNames(dataFrame).length
  }

  function rowCount(dataFrame: any): number {
    if (columnCount(dataFrame) === 0) {
      return 0
    }
    const firstColumn = dataFrame[columnNames(dataFrame)[0]]
    return Object.keys(firstColumn).length - 1
  }

  function rows(data: any): PropsUITableRow[] {
    const result: PropsUITableRow[] = []
    const n = rowCount(data)
    for (let row = 0; row <= n; row++) {
      const id = `${row}`
      const cells = columnNames(data).map((column: string) => rowCell(data, column, row))
      result.push({ __type__: 'PropsUITableRow', id, cells })
    }
    return result
  }

  function parseTables(
    tablesData: PropsUIPromptConsentFormTable[]
  ): Array<PropsUITable & TableContext> {
    return tablesData.map((table) => parseTable(table))
  }

  function parseTable(tableData: PropsUIPromptConsentFormTable): PropsUITable & TableContext {
    const id = tableData.id
    const title = Translator.translate(tableData.title, props.locale)
    const description = tableData.description
      ? Translator.translate(tableData.description, props.locale)
      : ''
    const dataFrame = JSON.parse(tableData.data_frame)
    const headCells = columnNames(dataFrame).map((column: string) => column)
    const head: PropsUITableHead = { __type__: 'PropsUITableHead', cells: headCells }
    const body: PropsUITableBody = { __type__: 'PropsUITableBody', rows: rows(dataFrame) }
    return {
      __type__: 'PropsUITable',
      id,
      head,
      body,
      title,
      description,
      deletedRowCount: 0,
      annotations: [],
      originalBody: body,
      deletedRows: [],
      visualizations: tableData.visualizations
    }
  }
}

interface Copy {
  description: string
  donateQuestion: string
  donateButton: string
  cancelButton: string
}

function prepareCopy({ locale }: Props): Copy {
  return {
    description: Translator.translate(description, locale),
    donateQuestion: Translator.translate(donateQuestionLabel, locale),
    donateButton: Translator.translate(donateButtonLabel, locale),
    cancelButton: Translator.translate(cancelButtonLabel, locale)
  }
}

const donateQuestionLabel = new TextBundle()
  .add('en', 'Do you want to donate the above data?')
  .add('nl', 'Wilt u de bovenstaande gegevens doneren?')

const donateButtonLabel = new TextBundle().add('en', 'Yes, donate').add('nl', 'Ja, doneer')

const cancelButtonLabel = new TextBundle().add('en', 'No').add('nl', 'Nee')

const description = new TextBundle()
  .add(
    'en',
    'Below is a table showing the cleaned data from your data package.\n' +
    'Before you share it with us, you can inspect what is in the tables, and decide what you do not want to share with us.\n' +
    'You can delete entries by clicking the checkbox next to them and clicking "remove". Navigate using the menu under "Type" and the search tool.\n' +
    'Once you have inspected it, click donate.'
  )
  .add(
    'nl',
    'Hieronder ziet u de gegevens die uit uw datapakketje gehaald worden. Op dit moment zijn er nog geen gegevens gedeeld met de onderzoekers van het Centerpanel. Voordat u deze gegevens deelt, kunt u ze bekijken en beslissen wat u wel of niet wilt delen. U kunt gegevens verwijderen door op het selectievakje ernaast te klikken en op Verwijder te klikken. Als u alles hebt gecontroleerd, klik dan op doneren onderaan de pagina.'
  )
