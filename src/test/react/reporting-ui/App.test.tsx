import { render, screen } from '@testing-library/react';
import App from '../../../main/react/reporting-ui/src/App';

// Used for __tests__/testing-library.js
// Learn more: https://github.com/testing-library/jest-dom
import React from 'react';
import '@testing-library/jest-dom/extend-expect';

test('renders learn react link', () => {
  render(<App />);
  const linkElement = screen.getByText(/learn react/i);
  expect(linkElement).toBeInTheDocument();
});
