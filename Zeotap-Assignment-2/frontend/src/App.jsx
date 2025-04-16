import React, { useState } from "react";
import axios from "axios";
import "./App.css";

function App() {
  const [sourceType, setSourceType] = useState("");
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [database, setDatabase] = useState("");
  const [user, setUser] = useState("");
  const [jwtToken, setJwtToken] = useState("");
  const [tables, setTables] = useState([]);
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("");
  const [selectedTable, setSelectedTable] = useState("");
  const [resultMessage, setResultMessage] = useState("");

  const handleSourceChange = (e) => {
    setSourceType(e.target.value);
    resetForm();
  };

  const resetForm = () => {
    setHost("");
    setPort("");
    setDatabase("");
    setUser("");
    setJwtToken("");
    setTables([]);
    setColumns([]);
    setSelectedColumns([]);
    setFile(null);
    setStatus("");
    setSelectedTable("");
    setResultMessage("");
  };

  const handleConnect = async (e) => {
    e.preventDefault();
    setStatus("Connecting to ClickHouse...");

    try {
      const response = await axios.post(
        "http://localhost:8000/connect_clickhouse",
        new URLSearchParams({ host, port, database, user, jwt_token: jwtToken }),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
      );

      if (response.data.status === "success") {
        setTables(response.data.tables);
        setStatus("Connected! Tables listed.");
      }
    } catch (err) {
      setStatus("Connection failed.");
      console.error(err);
    }
  };

  const handleLoadColumns = async (table) => {
    setSelectedTable(table);
    setStatus(`Fetching columns from ${table}...`);

    try {
      const response = await axios.post(
        "http://localhost:8000/get_columns",
        new URLSearchParams({ table, host, port, database, user, jwt_token: jwtToken }),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
      );

      setColumns(response.data.columns);
      setStatus("Columns loaded.");
    } catch (err) {
      setStatus("Failed to fetch columns.");
      console.error(err);
    }
  };

  const handleColumnSelection = (e) => {
    const col = e.target.name;
    setSelectedColumns((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]
    );
  };

  const handleClickhouseToFile = async () => {
    if (!selectedTable || selectedColumns.length === 0) {
      alert("Please select a table and at least one column.");
      return;
    }

    setStatus("Starting ingestion from ClickHouse to CSV...");

    try {
      const response = await axios.post(
        "http://localhost:8000/ingest_clickhouse_to_file",
        new URLSearchParams({
          table: selectedTable,
          columns: selectedColumns.join(","),
          host,
          port,
          database,
          user,
          jwt_token: jwtToken,
        }),
        { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
      );

      setResultMessage(`Ingestion successful! Records exported: ${response.data.records}`);
    } catch (err) {
      setResultMessage("Ingestion failed.");
      console.error(err);
    }
  };

  const handleFileUpload = async (e) => {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("table", selectedTable);
    formData.append("host", host);
    formData.append("port", port);
    formData.append("database", database);
    formData.append("user", user);
    formData.append("jwt_token", jwtToken);

    setStatus("Starting file upload to ClickHouse...");

    try {
      const response = await axios.post("http://localhost:8000/ingest_file_to_clickhouse", formData, {
        headers: { "Content-Type": "multipart/form-data" },
      });

      setResultMessage(`File ingestion successful! Records inserted: ${response.data.records}`);
    } catch (err) {
      setResultMessage("File ingestion failed.");
      console.error(err);
    }
  };

  return (
    <div className="App">
      <h1>ClickHouse Data Ingestion</h1>
      <form onSubmit={handleConnect}>
        <div>
          <label>Source Type:</label>
          <select value={sourceType} onChange={handleSourceChange}>
            <option value="">Select Source</option>
            <option value="clickhouse">ClickHouse</option>
            <option value="file">File</option>
          </select>
        </div>
        <div>
          <label>Host:</label>
          <input
            type="text"
            value={host}
            onChange={(e) => setHost(e.target.value)}
            placeholder="Enter ClickHouse Host"
          />
        </div>
        <div>
          <label>Port:</label>
          <input
            type="number"
            value={port}
            onChange={(e) => setPort(e.target.value)}
            placeholder="Enter Port"
          />
        </div>
        <div>
          <label>Database:</label>
          <input
            type="text"
            value={database}
            onChange={(e) => setDatabase(e.target.value)}
            placeholder="Enter Database"
          />
        </div>
        <div>
          <label>User:</label>
          <input
            type="text"
            value={user}
            onChange={(e) => setUser(e.target.value)}
            placeholder="Enter User"
          />
        </div>
        <div>
          <label>JWT Token:</label>
          <input
            type="text"
            value={jwtToken}
            onChange={(e) => setJwtToken(e.target.value)}
            placeholder="Enter JWT Token"
          />
        </div>
        <button type="submit">Connect</button>
      </form>

      {tables.length > 0 && (
        <div>
          <h3>Tables:</h3>
          <ul>
            {tables.map((table, index) => (
              <li key={index} onClick={() => handleLoadColumns(table)}>
                {table}
              </li>
            ))}
          </ul>
        </div>
      )}

      {columns.length > 0 && (
        <div>
          <h3>Columns:</h3>
          {columns.map((column, index) => (
            <label key={index}>
              <input
                type="checkbox"
                name={column}
                onChange={handleColumnSelection}
              />
              {column}
            </label>
          ))}
        </div>
      )}

      <button onClick={handleClickhouseToFile}>Ingest from ClickHouse to CSV</button>

      <input type="file" onChange={(e) => setFile(e.target.files[0])} />
      <button onClick={handleFileUpload}>Upload File to ClickHouse</button>

      <p>{status}</p>
      <p>{resultMessage}</p>
    </div>
  );
}

export default App;
