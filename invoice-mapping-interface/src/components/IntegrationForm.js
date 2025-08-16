import React, { useState } from 'react';
import './IntegrationForm.css';

function IntegrationForm({ onClose }) {
  const [host, setHost] = useState('');
  const [port, setPort] = useState('');
  const [schema, setSchema] = useState('');
  const [tableName, setTableName] = useState('');
  const [appId, setAppId] = useState('');
  const [mappings, setMappings] = useState([{ jsonKey: '', columnName: '' }]);

  const handleMappingChange = (index, field, value) => {
    const updated = [...mappings];
    updated[index][field] = value;
    setMappings(updated);
  };

  const handleAddRow = () => {
    setMappings([...mappings, { jsonKey: '', columnName: '' }]);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!host || !port || !schema || !tableName || !appId) {
      alert('Please fill in all fields.');
      return;
    }

    const mappingObject = {};
    mappings.forEach(({ jsonKey, columnName }) => {
      if (jsonKey && columnName) {
        mappingObject[jsonKey] = columnName;
      }
    });

    const payload = {
      host,
      port,
      schema,
      table_name: tableName,
      app_id: appId,
      mapping: mappingObject,
    };

    try {
      const response = await fetch('http://localhost:8000/save_connector_config/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      const result = await response.json();

      if (response.ok) {
        alert('✅ Configuration saved successfully!');
      } else {
        alert(`❌ Failed:\n${result.detail || JSON.stringify(result)}`);
      }
    } catch (error) {
      console.error('Error:', error);
      alert('❌ An unexpected error occurred.');
    }
  };

  return (
    <div className="form-wrapper">
      <button className="close-button" onClick={onClose}>&times;</button>
      <div className="form-header">
        <div className="header-left">
          <img
            src="https://getdrawings.com/free-icon-bw/json-icon-20.png"
            alt="JSON"
            width="40"
            height="40"
          />
          <h2>Integrate Excel with ViaEdge</h2>
        </div>
      </div>

      <form className="form1"onSubmit={handleSubmit}>
        <label>Host *</label>
        <input type="text" value={host} onChange={(e) => setHost(e.target.value)} placeholder="Enter host" required />

        <label>Port</label>
        <input type="text" value={port} onChange={(e) => setPort(e.target.value)} placeholder="Enter port" />

        <label>Schema</label>
        <input type="text" value={schema} onChange={(e) => setSchema(e.target.value)} placeholder="Enter schema" />

        <label>Table Name</label>
        <input type="text" value={tableName} onChange={(e) => setTableName(e.target.value)} placeholder="Enter table name" />

        <label>App_ID</label>
        <input type="text" value={appId} onChange={(e) => setAppId(e.target.value)} placeholder="Enter App ID" />

        <div className="table-wrapper">
          <table className="field-mapping-table">
  <thead>
    <tr>
      <th>JSON Key</th>
      <th>Column Name</th>
    </tr>
  </thead>
  <tbody>
    {mappings.map((mapping, index) => (
      <tr key={index}>
        <td>
          <input
            type="text"
            value={mapping.jsonKey}
            onChange={(e) => handleMappingChange(index, 'jsonKey', e.target.value)}
          />
        </td>
        <td>
          <input
            type="text"
            value={mapping.columnName}
            onChange={(e) => handleMappingChange(index, 'columnName', e.target.value)}
          />
        </td>
      </tr>
    ))}
  </tbody>
</table>

        </div>

        <div className="add-field" onClick={handleAddRow}>
  <span style={{ fontSize: '16px', lineHeight: '1' }}>＋</span>
  <span>Field Mapping</span>
</div>
<div className="button-row">
          <button type="button" className="cancel-btn" onClick={onClose}>Cancel</button>
          <button type="submit" className="integrate-btn">Integrate</button>
        </div>
        
      </form>
       
    </div>
    
  );
}

export default IntegrationForm;
