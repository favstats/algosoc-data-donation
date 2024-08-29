import React, { useCallback, useMemo, useState, useEffect, useRef } from 'react'
import { TableWithContext, PropsUITableRow } from '../../../../types/elements'
import { VisualizationType } from '../../../../types/visualizations'
import { Figure } from '../elements/figure'
import { TableItems } from './table_items'
import { SearchBar } from './search_bar'
import { Title4 } from './text'
import TextBundle from '../../../../text_bundle'
import { Translator } from '../../../../translator'
import { Table } from './table'

interface TableContainerProps {
  id: string
  table: TableWithContext
  updateTable: (tableId: string, table: TableWithContext) => void
  locale: string
}

export const TableContainer = ({
  id,
  table,
  updateTable,
  locale
}: TableContainerProps): JSX.Element => {
  const tableVisualizations = table.visualizations != null ? table.visualizations : []
  const [searchFilterIds, setSearchFilterIds] = useState<Set<string>>()
  const [dataTypeFilter, setDataTypeFilter] = useState<string | null>(null)
  const [search, setSearch] = useState<string>('')
  const autoOpen = useRef<boolean>(true)
  const text = useMemo(() => getTranslations(locale), [locale])
  const [show, setShow] = useState<boolean>(true)

  useEffect(() => {
    const timer = setTimeout(() => {
      const ids = searchRows(table.originalBody.rows, search, dataTypeFilter)
      setSearchFilterIds(ids)
      if ((search !== '' || dataTypeFilter !== null) && autoOpen.current) {
        autoOpen.current = false
        setTimeout(() => setShow(true), 10)
      }
    }, 300)
    return () => clearTimeout(timer)
  }, [search, dataTypeFilter, table.originalBody.rows])

  const searchedTable = useMemo(() => {

    if (searchFilterIds === undefined) return table
    const filteredRows = table.body.rows.filter((row) => searchFilterIds.has(row.id))
    return { ...table, body: { ...table.body, rows: filteredRows } }
  }, [table, searchFilterIds, dataTypeFilter])

  const handleDelete = useCallback(
    (rowIds?: string[]) => {
      if (rowIds == null) {
        if (searchedTable !== null) {
          rowIds = searchedTable.body.rows.map((row) => row.id)
        } else {
          return
        }
      }
      if (rowIds.length > 0) {
        if (rowIds.length === searchedTable?.body?.rows?.length) {
          setSearch('')
          setDataTypeFilter(null)
          setSearchFilterIds(undefined)
        }
        const deletedRows = [...table.deletedRows, rowIds]
        const newTable = deleteTableRows(table, deletedRows)
        updateTable(id, newTable)
      }
    },
    [id, table, searchedTable, updateTable]
  )

  const handleUndo = useCallback(() => {
    const deletedRows = table.deletedRows.slice(0, -1)
    const newTable = deleteTableRows(table, deletedRows)
    updateTable(id, newTable)
  }, [id, table, updateTable])

  const unfilteredRows = table.body.rows.length

  const handleDataTypeFilter = useCallback((dataType: string) => {
    setDataTypeFilter(dataType === '' ? null : dataType)
  }, [])

  return (
    <div
      key={table.id}
      className="p-3 md:p-4 lg:p-6 flex flex-col gap-4 w-full overflow-hidden border-[0.2rem] border-grey4 rounded-lg"
    >
      <div className="flex flex-wrap ">
        <div key="Title" className="flex sm:flex-row justify-between w-full gap-1 mb-2">
          <Title4 text={table.title} margin="" />

          {unfilteredRows > 0 ? (
            <SearchBar placeholder={text.searchPlaceholder} search={search} onSearch={setSearch} />
          ) : null}
        </div>
        <div
          key="Description"
          className="flex flex-col w-full mb-2 text-base md:text-lg font-body max-w-2xl"
        >
          <p>{table.description}</p>
        </div>
        <div
          key="TableSummary"
          className="flex items-center justify-between w-full mt-1 pt-1 rounded "
        >
          <TableItems
            table={table}
            searchedTable={searchedTable}
            handleUndo={handleUndo}
            locale={locale}
            dataTypeFilter={dataTypeFilter}
          />

          <button
            key={show ? 'animate' : ''}
            className={`flex end gap-3 animate-fadeIn ${unfilteredRows === 0 ? 'hidden' : ''}`}
            onClick={() => setShow(!show)}
          >
            <div key="zoomIcon" className="text-primary">
              {show ? zoomOutIcon : zoomInIcon}
            </div>
            <div key="zoomText" className="text-right hidden md:block">
              {show ? text.hideTable : text.showTable}
            </div>
          </button>
        </div>
        <div key="Table" className="w-full">
          <div className="">
            <Table
              show={show}
              table={searchedTable}
              search={search}
              unfilteredRows={unfilteredRows}
              handleDelete={handleDelete}
              handleUndo={handleUndo}
              locale={locale}
              dataTypeFilter={dataTypeFilter}
              onDataTypeFilter={handleDataTypeFilter}
            />
          </div>
        </div>
        <div
          key="Visualizations"
          className={`pt-2 grid w-full gap-4 transition-all ${
            tableVisualizations.length > 0 && unfilteredRows > 0 ? '' : 'hidden'
          }`}
        >
          {tableVisualizations.map((vs: VisualizationType, i: number) => {
            return (
              <Figure
                key={table.id + '_' + String(i)}
                table={searchedTable}
                visualization={vs}
                locale={locale}
                handleDelete={handleDelete}
                handleUndo={handleUndo}
              />
            )
          })}
        </div>
      </div>
    </div>
  )
}

function searchRows(rows: PropsUITableRow[], search: string, dataTypeFilter: string | null): Set<string> | undefined {
  if (search.trim() === '' && dataTypeFilter === null) return undefined

  const query = [search.trim()]
  const regexes: RegExp[] = []
  for (const q of query) regexes.push(new RegExp(q.replace(/[-/\\^$*+?.()|[\]{}]/, '\\$&'), 'i'))

  const ids = new Set<string>()
  for (const row of rows) {
    let matchesSearch = search.trim() === ''
    let matchesDataType = dataTypeFilter === null

    for (const regex of regexes) {
      for (const cell of row.cells) {
        if (regex.test(cell)) {
          matchesSearch = true
          break
        }
      }
      if (matchesSearch) break
    }

    const dataTypeIndex = row.cells.findIndex((cell, index) => index === 0) // Assuming Type is the first column
    if (dataTypeIndex !== -1 && (dataTypeFilter === null || row.cells[dataTypeIndex] === dataTypeFilter)) {
      matchesDataType = true
    }

    if (matchesSearch && matchesDataType) {
      ids.add(row.id)
    }
  }

  return ids
}

function deleteTableRows(table: TableWithContext, deletedRows: string[][]): TableWithContext {
  const deleteIds = new Set<string>()
  for (const deletedSet of deletedRows) {
    for (const id of deletedSet) {
      deleteIds.add(id)
    }
  }

  const rows = table.originalBody.rows.filter((row) => !deleteIds.has(row.id))
  const deletedRowCount = table.originalBody.rows.length - rows.length
  return { ...table, body: { ...table.body, rows }, deletedRowCount, deletedRows }
}

const zoomInIcon = (
  <svg
    className="h-6 w-6"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    viewBox="0 0 24 24"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607zM10.5 7.5v6m3-3h-6"
    />
  </svg>
)

const zoomOutIcon = (
  <svg
    className="h-6 w-6"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    viewBox="0 0 24 24"
    xmlns="http://www.w3.org/2000/svg"
    aria-hidden="true"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607zM13.5 10.5h-6"
    />
  </svg>
)

function getTranslations(locale: string): Record<string, string> {
  const translated: Record<string, string> = {}
  for (const [key, value] of Object.entries(translations)) {
    translated[key] = Translator.translate(value, locale)
  }
  return translated
}

const translations = {
  searchPlaceholder: new TextBundle().add('en', 'Search').add('nl', 'Zoeken'),
  showTable: new TextBundle().add('en', 'Show table').add('nl', 'Tabel tonen'),
  hideTable: new TextBundle().add('en', 'Hide table').add('nl', 'Tabel verbergen')
}