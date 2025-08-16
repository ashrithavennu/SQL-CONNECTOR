import React, { useState } from 'react';
import './App.css';
import IntegrationForm from './components/IntegrationForm';

function App() {
  const [showForm, setShowForm] = useState(false);

  const toggleForm = () => {
    setShowForm(!showForm);
  };

  return (
    <div className="App">
      {!showForm && (
        <button className="click-me" onClick={toggleForm}>
          Click Me
        </button>
      )}

      <div className={`slide-container ${showForm ? 'active' : ''}`}>
        {showForm && <IntegrationForm onClose={toggleForm} />}
      </div>
    </div>
  );
}

export default App;
