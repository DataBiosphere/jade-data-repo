import React, { Component } from 'react';
import jade from './jade.png';
import './App.css';

class App extends Component {
  render() {
    return (
      <div className="App">
        <header className="App-header">
          <img src={jade} className="App-logo" alt="jade" />
          <p>
            Welcome to the Jade Data Repository!
          </p>
          <a
            className="App-link"
            href="https://github.com/DataBiosphere/jade-data-repo"
            target="_blank"
            rel="noopener noreferrer"
          >
            See the Jade source code
          </a>
        </header>
      </div>
    );
  }
}

export default App;
