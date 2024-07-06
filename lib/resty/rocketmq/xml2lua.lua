--- @module Module providing a non-validating XML stream parser in Lua.
--
--  Features:
--  =========
--
--      * Tokenises well-formed XML (relatively robustly)
--      * Flexible handler based event API (see below)
--      * Parses all XML Infoset elements - ie.
--          - Tags
--          - Text
--          - Comments
--          - CDATA
--          - XML Decl
--          - Processing Instructions
--          - DOCTYPE declarations
--      * Provides limited well-formedness checking
--        (checks for basic syntax & balanced tags only)
--      * Flexible whitespace handling (selectable)
--      * Entity Handling (selectable)
--
--  Limitations:
--  ============
--
--      * Non-validating
--      * No charset handling
--      * No namespace support
--      * Shallow well-formedness checking only (fails
--        to detect most semantic errors)
--
--  API:
--  ====
--
--  The parser provides a partially object-oriented API with
--  functionality split into tokeniser and handler components.
--
--  The handler instance is passed to the tokeniser and receives
--  callbacks for each XML element processed (if a suitable handler
--  function is defined). The API is conceptually similar to the
--  SAX API but implemented differently.
--
--  XML data is passed to the parser instance through the 'parse'
--  method (Note: must be passed a single string currently)
--
--  License:
--  ========
--
--      This code is freely distributable under the terms of the [MIT license](LICENSE).
--
--
--@author Paul Chakravarti (paulc@passtheaardvark.com)
--@author Manoel Campos da Silva Filho
local xml2lua = { _VERSION = "1.6-1" }

---Gets an _attr element from a table that represents the attributes of an XML tag,
--and generates a XML String representing the attibutes to be inserted
--into the openning tag of the XML
--
--@param attrTable table from where the _attr field will be got
--@return a XML String representation of the tag attributes
local function attrToXml(attrTable)
  local s = ""
  attrTable = attrTable or {}

  for k, v in pairs(attrTable) do
      s = s .. " " .. k .. "=" .. '"' .. v .. '"'
  end
  return s
end

---Gets the first key of a given table
local function getSingleChild(tb)
  local count = 0
  for _ in pairs(tb) do
    count = count + 1
  end
  if (count == 1) then
      for k, _ in pairs(tb) do
          return k
      end
  end
  return nil
end

---Gets the first value of a given table
local function getFirstValue(tb)
  if type(tb) == "table" then
    for _, v in pairs(tb) do
      return v
    end
      return nil
   end

   return tb
end

xml2lua.pretty = true

function xml2lua.getSpaces(level)
  local spaces = ''
  if (xml2lua.pretty) then
    spaces = string.rep(' ', level * 2)
  end
  return spaces
end

function xml2lua.addTagValueAttr(xmltb, tagName, tagValue, attrTable, level)
  local attrStr = attrToXml(attrTable)
  local spaces = xml2lua.getSpaces(level)
  if (tagValue == '') then
    table.insert(xmltb, spaces .. '<' .. tagName .. attrStr .. '/>')
  else
    table.insert(xmltb, spaces .. '<' .. tagName .. attrStr .. '>' .. tostring(tagValue) .. '</' .. tagName .. '>')
  end
end

function xml2lua.startTag(xmltb, tagName, attrTable, level)
  local attrStr = attrToXml(attrTable)
  local spaces = xml2lua.getSpaces(level)
  if (tagName ~= nil) then
    table.insert(xmltb, spaces .. '<' .. tagName .. attrStr .. '>')
  end
end

function xml2lua.endTag(xmltb, tagName, level)
  local spaces = xml2lua.getSpaces(level)
  if (tagName ~= nil) then
    table.insert(xmltb, spaces .. '</' .. tagName .. '>')
  end
end

function xml2lua.isChildArray(obj)
  for tag, _ in pairs(obj) do
    if (type(tag) == 'number') then
      return true
    end
  end
  return false
end

function xml2lua.isTableEmpty(obj)
  for k, _ in pairs(obj) do
    if (k ~= '_attr') then
      return false
    end
  end
  return true
end

function xml2lua.parseTableToXml(xmltb, obj, tagName, level)
  if (tagName ~= '_attr') then
    if (type(obj) == 'table') then
      if (xml2lua.isChildArray(obj)) then
        for _, value in pairs(obj) do
          xml2lua.parseTableToXml(xmltb, value, tagName, level)
        end
      elseif xml2lua.isTableEmpty(obj) then
        xml2lua.addTagValueAttr(xmltb, tagName, "", obj._attr, level)
      else
        xml2lua.startTag(xmltb, tagName, obj._attr, level)
        for tag, value in pairs(obj) do
          xml2lua.parseTableToXml(xmltb, value, tag, level + 1)
        end
        xml2lua.endTag(xmltb, tagName, level)
      end
    else
      xml2lua.addTagValueAttr(xmltb, tagName, obj, nil, level)
    end
  end
    end

---Converts a Lua table to a XML String representation.
--@param tb Table to be converted to XML
--@param tableName Name of the table variable given to this function,
--                 to be used as the root tag. If a value is not provided
--                 no root tag will be created.
--@param level Only used internally, when the function is called recursively to print indentation
--
--@return a String representing the table content in XML
function xml2lua.toXml(tb, tableName, level)
  local xmltb = {}
  level = level or 0
  local singleChild = getSingleChild(tb)
  tableName = tableName or singleChild

  if (singleChild) then
    xml2lua.parseTableToXml(xmltb, getFirstValue(tb), tableName, level)
            else
    xml2lua.parseTableToXml(xmltb, tb, tableName, level)
  end

  if (xml2lua.pretty) then
    return table.concat(xmltb, '\n')
  end
  return table.concat(xmltb)
end

return xml2lua
