let XLSX = require('xlsx')

let targetTemplate

const parseDelimitedValue = (row, delimiter, splitType) => {
  let parts = row.split(delimiter)
  let keys = getTargetKeysForType(splitType)

  let parsedResult = []

  for (let i = 0; i < parts.length; i++) {
    let value = parts[i]
    if (splitType === 'DX') {
      value = value.replace('.', '')
    }

    parsedResult.push({
      key: keys[i],
      value: value
    })
  }

  return parsedResult
}

const getModifiersPivotTable = (list, sourceColumns, pivotType) => {
  const firstSourceCol = sourceColumns[0]
  let rowsWithPivotValues = list.filter(x => x[firstSourceCol] && x[firstSourceCol].toUpperCase() !== 'NULL')
  let keys = getTargetKeysForType(pivotType)

  let pivotTable = []

  for (let i = 0; i < rowsWithPivotValues.length; i++) {
    let concattedMods = []
    sourceColumns.forEach(x => {
      let value = rowsWithPivotValues[i][x]
      if (value && value !== 'NULL') {
        concattedMods.push(value.trim())
      }
    })

    if (concattedMods.length > 0) {
      pivotTable.push({
        key: keys[i],
        value: concattedMods.join(',')
      })
    }
  }

  return pivotTable
}

const getPivotTable = (list, source, pivotType) => {
  let rowsWithPivotValues = list.filter(x => x[source] && x[source].toUpperCase() !== 'NULL')

  let keys = getTargetKeysForType(pivotType)
  let pivotTable = []

  for (let i = 0; i < rowsWithPivotValues.length; i++) {
    let value = rowsWithPivotValues[i][source].trim()
    if (pivotType === 'DX') {
      value = value.replace('.', '')
    }
    if (keys[i]) {
      pivotTable.push({
        key: keys[i],
        value: value
      })
    }
  }

  return pivotTable
}

const getTargetKeysForType = (targetType) => {
  let keys = getTargetKeys().filter(x => x.toUpperCase().includes(targetType))
  return keys
}

const getTargetKeys = () => {
  let targetKeys = Object.keys(targetTemplate)
  targetKeys = targetKeys.map(x => x.charAt(0).toUpperCase() + x.slice(1))
  return targetKeys
}

const mapList = (msg, list) => {
  let pullListBuild = msg.data.pullListBuild
  targetTemplate = msg.data.targetTemplate
  list = list ? list : msg.data.list

  // trim all strings
  list = list.map(obj => {
    Object.keys(obj).map(k => obj[k] = typeof obj[k] == 'string' ? obj[k].trim() : obj[k])
    return obj
  })

  let mappedColumns = pullListBuild.mapping
    .filter(x => (x.target && x.target.length > 0) || x.splitType || x.pivotType)

  if (pullListBuild.recordRowStyle === 'Multi') {
    // fill sparsely populated rows like VALLE example
    let trackerRow = {}
    for (let i = 0; i < list.length; i++) {
      if (list[i][pullListBuild.patientNumberField] && list[i][pullListBuild.dischargeDateField]) {
        trackerRow = list[i]
      }

      if (!list[i][pullListBuild.patientNumberField]) {
        let row = list[i]
        Object.keys(trackerRow).forEach(x => {
          // don't fill pivoted rows
          let mapping = mappedColumns.find(col => col.source === x)
          if (!row[x] && mapping && !mapping.pivotType) {
            row[x] = trackerRow[x]
          }
        })
      }
    }

    let patNums = [...new Set(list.map(x => {
      return JSON.stringify({
        patNum: x[pullListBuild.patientNumberField],
        dischargeDate: x[pullListBuild.dischargeDateField]
      })
    }))]

    postMessage({ type: 'status', statusMessage: `Found ${patNums.length} Patient Numbers` })

    let mappedList = []

    let stamp = new Date()
    for (let i = 0; i < patNums.length; i++) {
      if ((i + 1) % 100 === 0) {
        postMessage({ type: 'status', statusMessage: `Done with ${i + 1} Patient Numbers in ${((new Date().getTime() - stamp.getTime()) / 1000)}s` })
        stamp = new Date()
      }

      let uniquePatNumRow = JSON.parse(patNums[i])
      let mappedRow = {}
      let patNumRows = list.filter(x => x[pullListBuild.patientNumberField] === uniquePatNumRow.patNum && x[pullListBuild.dischargeDateField] === uniquePatNumRow.dischargeDate)
      let x = patNumRows[0]

      mappedColumns.forEach(y => {
        if (y.target && y.target.length > 0) {
          let value = x[y.source] ? x[y.source].trim() : null
          if (y.script && value) {
            let fn = eval(`(sourceValue, row) => {${y.script}}`) // eslint-disable-line
            value = fn(value, x)
            mappedRow[y.target[0]] = value
          } else {
            if (y.target[0].toUpperCase().includes('DX') && value) {
              value = value.replace('.', '')
            }
          }

          if (y.type === 'constant') {
            mappedRow[y.target[0]] = y.constantValue
          } else {
            mappedRow[y.target[0]] = value
          }
        }

        if (y.pivotType) {
          let pivotTable
          if (y.pivotType === 'MOD') {
            pivotTable = getModifiersPivotTable(patNumRows, y.modifierSourceColumns, y.pivotType)
          } else {
            pivotTable = getPivotTable(patNumRows, y.source, y.pivotType)
          }

          pivotTable.forEach(z => {
            mappedRow[z.key] = z.value
          })
        }

        if (y.splitType) {
          let delimitedSplit = parseDelimitedValue(x[y.source], y.delimiter, y.splitType)
          delimitedSplit.forEach(z => {
            mappedRow[z.key] = z.value
          })
        }
      })

      mappedList.push(mappedRow)
      list = list.filter(x => {
        if (x[pullListBuild.patientNumberField] === uniquePatNumRow.patNum && x[pullListBuild.dischargeDateField] === uniquePatNumRow.dischargeDate) {
          return false
        }
        return true
      })
    }
    return mappedList
  }

  return list.map(x => {
    let mappedRow = {}

    mappedColumns.forEach(y => {
      if (y.target && y.target.length) {
        let value = x[y.source] ? x[y.source].trim() : null
        if (y.script && value) {
          let fn = eval(`(sourceValue, row) => {${y.script}}`) // eslint-disable-line
          value = fn(value, x)
          mappedRow[y.target[0]] = value
        } else {
          if (y.target[0].toUpperCase().includes('DX') && value) {
            value = value.replace('.', '')
          }
        }

        if (y.type === 'constant') {
          mappedRow[y.target[0]] = y.constantValue
        } else {
          mappedRow[y.target[0]] = value
        }
      }

      if (y.delimitedSplit && y.delimitedSplit.length > 0) {
        let delimitedSplit = parseDelimitedValue(x[y.source], y.delimiter, y.splitType)
        delimitedSplit.forEach(z => {
          mappedRow[z.key] = z.value
        })
      }
    })
    return mappedRow
  })
}

const readFile = async (msg) => {
  let importFile = msg.data.importFile
  let sheetName = msg.data.sheetName
  let opts = msg.data.opts

  const data = await importFile.arrayBuffer()
  const workbook = XLSX.read(data, opts)

  if (!sheetName) {
    sheetName = workbook.SheetNames[0]
  }

  const json = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { cellText: false, raw: false })
  return { workbook, json }
}

onmessage = async (msg) => {
  console.log('got message in NPM module')
  if (msg.data.operation === 'readFile') {
    let response = await readFile(msg)
    postMessage(response)
  }

  if (msg.data.operation === 'mapList') {
    let mappedList = mapList(msg)
    postMessage(mappedList)
  }

  if (msg.data.operation === 'readFileAndMapList') {
    let response = await readFile(msg)
    postMessage({ type: 'status', statusMessage: 'Done reading file' })
    postMessage({ type: 'status', statusMessage: 'Mapping list' })
    let mappedList = mapList(msg, response.json)
    postMessage(mappedList)
  }
}
