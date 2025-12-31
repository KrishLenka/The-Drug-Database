import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_BACKEND_URL || 'http://localhost:8001';

// Dataset configurations
const datasetConfigs = {
  products: {
    name: 'Products',
    description: 'FDA approved drug products',
    dateFields: ['approval_date'],
    columns: ['appl_no', 'appl_type', 'ingredient', 'dosage', 'form', 'route', 'trade_name', 'applicant', 'strength', 'te_code', 'approval_date', 'rld', 'rs', 'type', 'applicant_full_name', 'number_of_approvals']
  },
  exclusivity: {
    name: 'Exclusivity',
    description: 'Drug exclusivity records',
    dateFields: ['exclusivity_date'],
    columns: ['appl_no', 'appl_type', 'ingredient', 'dosage', 'form', 'route', 'trade_name', 'strength', 'exclusivity_code', 'exclusivity_date']
  },
  patent: {
    name: 'Patents',
    description: 'Drug patent information',
    dateFields: ['submission_date', 'patent_expire_date_text'],
    columns: ['appl_no', 'appl_type', 'ingredient', 'dosage', 'form', 'route', 'trade_name', 'applicant', 'strength', 'patent_no', 'patent_expire_date_text', 'drug_substance_flag', 'drug_product_flag', 'patent_use_code', 'submission_date']
  },
  sales: {
    name: 'Sales',
    description: 'Drug sales data',
    dateFields: [],
    columns: ['appl_no', 'ingredient', 'route', 'route_extended', 'dosage', 'manufacturer', 'strength', 'pack_quantity', 'ndc_number', 'labeler_code', 'product_code', 'sales', 'packs', 'quantity', 'wac', 'price', 'number_of_sellers']
  }
};

// Autocomplete Input Component
function AutocompleteInput({ field, label, value, onChange, options = [], placeholder, testId }) {
  const [isOpen, setIsOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);
  const inputRef = React.useRef(null);
  const dropdownRef = React.useRef(null);

  // Filter options based on input value
  const filteredOptions = React.useMemo(() => {
    if (!value) return options.slice(0, 50); // Show first 50 when empty
    const lowerValue = value.toLowerCase();
    return options.filter(opt => 
      String(opt).toLowerCase().includes(lowerValue)
    ).slice(0, 50);
  }, [value, options]);

  const handleInputChange = (e) => {
    onChange(e.target.value);
    setIsOpen(true);
    setHighlightedIndex(-1);
  };

  const handleSelect = (option) => {
    onChange(String(option));
    setIsOpen(false);
    inputRef.current?.blur();
  };

  const handleKeyDown = (e) => {
    if (!isOpen && e.key !== 'Escape') {
      setIsOpen(true);
      return;
    }

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setHighlightedIndex(prev => 
          prev < filteredOptions.length - 1 ? prev + 1 : prev
        );
        break;
      case 'ArrowUp':
        e.preventDefault();
        setHighlightedIndex(prev => prev > 0 ? prev - 1 : -1);
        break;
      case 'Enter':
        e.preventDefault();
        if (highlightedIndex >= 0 && filteredOptions[highlightedIndex]) {
          handleSelect(filteredOptions[highlightedIndex]);
        }
        break;
      case 'Escape':
        setIsOpen(false);
        inputRef.current?.blur();
        break;
    }
  };

  // Close dropdown when clicking outside
  React.useEffect(() => {
    const handleClickOutside = (event) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(event.target) &&
        inputRef.current &&
        !inputRef.current.contains(event.target)
      ) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div className="autocomplete-wrapper filter-group">
      <label>{label}</label>
      <div className="autocomplete-container" ref={dropdownRef}>
        <input
          ref={inputRef}
          type="text"
          value={value || ''}
          onChange={handleInputChange}
          onFocus={() => setIsOpen(true)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          data-testid={testId}
          className="autocomplete-input"
        />
        {isOpen && filteredOptions.length > 0 && (
          <ul className="autocomplete-dropdown">
            {filteredOptions.map((option, index) => (
              <li
                key={index}
                className={`autocomplete-option ${index === highlightedIndex ? 'highlighted' : ''}`}
                onClick={() => handleSelect(option)}
                onMouseEnter={() => setHighlightedIndex(index)}
              >
                {String(option)}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function App() {
  const [dataset, setDataset] = useState('products');
  const [textQuery, setTextQuery] = useState('');
  const [filters, setFilters] = useState({});
  const [dateRanges, setDateRanges] = useState({});
  const [filterOptions, setFilterOptions] = useState({});
  const [data, setData] = useState([]);
  const [pagination, setPagination] = useState({ page: 1, limit: 50, total: 0, totalPages: 0 });
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState({});
  const [sortBy, setSortBy] = useState('');
  const [sortOrder, setSortOrder] = useState('ASC');
  const [expandedFilters, setExpandedFilters] = useState(true);

  // Fetch filter options when dataset changes
  useEffect(() => {
    const fetchOptions = async () => {
      try {
        const response = await axios.get(`${API_URL}/api/filter-options/${dataset}`);
        setFilterOptions(response.data);
      } catch (err) {
        console.error('Failed to fetch filter options:', err);
      }
    };
    fetchOptions();
    setFilters({});
    setDateRanges({});
    setTextQuery('');
    setSortBy('');
  }, [dataset]);

  // Fetch stats on mount
  useEffect(() => {
    const fetchStats = async () => {
      try {
        const response = await axios.get(`${API_URL}/api/stats`);
        setStats(response.data);
      } catch (err) {
        console.error('Failed to fetch stats:', err);
      }
    };
    fetchStats();
  }, []);

  // Search function
  const search = useCallback(async (page = 1) => {
    setLoading(true);
    try {
      const response = await axios.post(`${API_URL}/api/search/${dataset}`, {
        textQuery,
        filters,
        dateRanges,
        page,
        limit: pagination.limit,
        sortBy,
        sortOrder
      });
      setData(response.data.data);
      setPagination(response.data.pagination);
    } catch (err) {
      console.error('Search failed:', err);
    } finally {
      setLoading(false);
    }
  }, [dataset, textQuery, filters, dateRanges, pagination.limit, sortBy, sortOrder]);

  // Search on filter change
  useEffect(() => {
    search(1);
  }, [dataset, filters, dateRanges, sortBy, sortOrder]);

  const handleTextSearch = (e) => {
    e.preventDefault();
    search(1);
  };

  const handleFilterChange = (field, value) => {
    setFilters(prev => ({
      ...prev,
      [field]: value
    }));
  };

  const handleDateRangeChange = (field, type, value) => {
    setDateRanges(prev => ({
      ...prev,
      [field]: {
        ...prev[field],
        [type]: value || undefined
      }
    }));
  };

  const clearFilters = () => {
    setFilters({});
    setDateRanges({});
    setTextQuery('');
    setSortBy('');
    setSortOrder('ASC');
  };

  const handleSort = (column) => {
    if (sortBy === column) {
      setSortOrder(prev => prev === 'ASC' ? 'DESC' : 'ASC');
    } else {
      setSortBy(column);
      setSortOrder('ASC');
    }
  };

  const config = datasetConfigs[dataset];

  // Function to generate Google Patent URL
  const getPatentUrl = (patentNo) => {
    if (!patentNo || patentNo === '-') return null;
    
    // Clean the patent number (remove whitespace, etc.)
    const cleanPatent = String(patentNo).trim();
    
    // Google Patents URL format: https://patents.google.com/patent/US{number}
    // Handle different patent formats
    if (cleanPatent.startsWith('US')) {
      // Already has US prefix
      return `https://patents.google.com/patent/${cleanPatent}`;
    } else if (/^\d+$/.test(cleanPatent)) {
      // Just numbers, assume US patent
      return `https://patents.google.com/patent/US${cleanPatent}`;
    } else {
      // Other formats (international patents, etc.)
      return `https://patents.google.com/patent/${cleanPatent}`;
    }
  };

  // Function to render table cell content
  const renderCellContent = (col, value) => {
    // If it's a patent number column and we have a value, render as link
    if (col === 'patent_no' && value && value !== '-') {
      const patentUrl = getPatentUrl(value);
      if (patentUrl) {
        return (
          <a 
            href={patentUrl} 
            target="_blank" 
            rel="noopener noreferrer"
            className="patent-link"
            onClick={(e) => e.stopPropagation()} // Prevent row click events
          >
            {value}
          </a>
        );
      }
    }
    // Default: just return the value
    return value || '-';
  };

  return (
    <div className="app" data-testid="drug-database">
      {/* Header */}
      <header className="header" data-testid="app-header">
        <div className="header-content">
          <h1>The Drug Database</h1>
          <p className="subtitle">Advanced pharmaceutical data filtering</p>
        </div>
      </header>

      <main className="main-content">
        {/* Dataset Selector */}
        <section className="dataset-section" data-testid="dataset-section">
          <h2>Select Dataset</h2>
          <div className="dataset-cards">
            {Object.entries(datasetConfigs).map(([key, cfg]) => (
              <button
                key={key}
                className={`dataset-card ${dataset === key ? 'active' : ''}`}
                onClick={() => setDataset(key)}
                data-testid={`dataset-${key}`}
              >
                <h3>{cfg.name}</h3>
                <p>{cfg.description}</p>
                <span className="record-count">{stats[key]?.toLocaleString() || '...'} records</span>
              </button>
            ))}
          </div>
        </section>

        {/* Text Query Search */}
        <section className="search-section" data-testid="search-section">
          <h2>Natural Language Query</h2>
          <form onSubmit={handleTextSearch} className="search-form">
            <input
              type="text"
              value={textQuery}
              onChange={(e) => setTextQuery(e.target.value)}
              placeholder='e.g., "products that include Aerosol and are approved after 2020" or "form is TABLET and route is ORAL"'
              className="search-input"
              data-testid="text-query-input"
            />
            <button type="submit" className="search-button" data-testid="search-button">
              Search
            </button>
          </form>
          <div className="search-hints">
            <p><strong>Examples:</strong></p>
            <ul>
              <li>Products approved after 2020</li>
              <li>Include BUDESONIDE with form AEROSOL</li>
              <li>Trade name XYREM</li>
              <li>Route is ORAL and type is RX</li>
            </ul>
          </div>
        </section>

        {/* Filters Section */}
        <section className="filters-section" data-testid="filters-section">
          <div className="filters-header">
            <h2>Advanced Filters</h2>
            <div className="filter-actions">
              <button 
                className="toggle-filters"
                onClick={() => setExpandedFilters(!expandedFilters)}
                data-testid="toggle-filters"
              >
                {expandedFilters ? 'Hide Filters' : 'Show Filters'}
              </button>
              <button className="clear-filters" onClick={clearFilters} data-testid="clear-filters">
                Clear All
              </button>
            </div>
          </div>

          {expandedFilters && (
            <div className="filters-grid" data-testid="filters-grid">
              {/* Date Range Filters */}
              {config.dateFields.map(field => (
                <div key={field} className="filter-group date-filter">
                  <label>{field.replace(/_/g, ' ').toUpperCase()}</label>
                  <div className="date-range">
                    <input
                      type="date"
                      value={dateRanges[field]?.from || ''}
                      onChange={(e) => handleDateRangeChange(field, 'from', e.target.value)}
                      placeholder="From"
                      data-testid={`date-from-${field}`}
                    />
                    <span>to</span>
                    <input
                      type="date"
                      value={dateRanges[field]?.to || ''}
                      onChange={(e) => handleDateRangeChange(field, 'to', e.target.value)}
                      placeholder="To"
                      data-testid={`date-to-${field}`}
                    />
                  </div>
                </div>
              ))}

              {/* Dropdown Filters */}
              {Object.entries(filterOptions).map(([field, options]) => (
                <div key={field} className="filter-group">
                  <label>{field.replace(/_/g, ' ').toUpperCase()}</label>
                  <select
                    multiple
                    value={filters[field] || []}
                    onChange={(e) => {
                      const selected = Array.from(e.target.selectedOptions, opt => opt.value);
                      handleFilterChange(field, selected);
                    }}
                    className="multi-select"
                    data-testid={`filter-${field}`}
                  >
                    {options.map(opt => (
                      <option key={opt} value={opt}>{opt}</option>
                    ))}
                  </select>
                  <small>Hold Control to select multiple/deselect</small>
                </div>
              ))}

              {/* Text search filters with autocomplete */}
              <AutocompleteInput
                field="ingredient"
                label="INGREDIENT (text search)"
                value={filters.ingredient || ''}
                onChange={(value) => handleFilterChange('ingredient', value)}
                options={filterOptions.ingredient || []}
                placeholder="Search ingredient..."
                testId="filter-ingredient-text"
              />

              <AutocompleteInput
                field="trade_name"
                label="TRADE NAME (text search)"
                value={filters.trade_name || ''}
                onChange={(value) => handleFilterChange('trade_name', value)}
                options={filterOptions.trade_name || []}
                placeholder="Search trade name..."
                testId="filter-trade-name-text"
              />

              <AutocompleteInput
                field="appl_no"
                label="APPLICATION NUMBER"
                value={filters.appl_no || ''}
                onChange={(value) => handleFilterChange('appl_no', value)}
                options={filterOptions.appl_no || []}
                placeholder="Search appl_no..."
                testId="filter-appl-no"
              />

              {/* Dataset-specific text filters */}
              {dataset === 'products' && (
                <AutocompleteInput
                  field="number_of_approvals"
                  label="NUMBER OF APPROVALS"
                  value={filters.number_of_approvals || ''}
                  onChange={(value) => handleFilterChange('number_of_approvals', value)}
                  options={filterOptions.number_of_approvals || []}
                  placeholder="Search number of approvals..."
                  testId="filter-number-of-approvals"
                />
              )}

              {dataset === 'exclusivity' && (
                <AutocompleteInput
                  field="exclusivity_code"
                  label="EXCLUSIVITY CODE (text search)"
                  value={filters.exclusivity_code || ''}
                  onChange={(value) => handleFilterChange('exclusivity_code', value)}
                  options={filterOptions.exclusivity_code || []}
                  placeholder="Search exclusivity code..."
                  testId="filter-exclusivity-code"
                />
              )}

              {dataset === 'patent' && (
                <AutocompleteInput
                  field="patent_use_code"
                  label="PATENT USE CODE (text search)"
                  value={filters.patent_use_code || ''}
                  onChange={(value) => handleFilterChange('patent_use_code', value)}
                  options={filterOptions.patent_use_code || []}
                  placeholder="Search patent use code..."
                  testId="filter-patent-use-code"
                />
              )}

              {dataset === 'sales' && (
                <AutocompleteInput
                  field="manufacturer"
                  label="MANUFACTURER (text search)"
                  value={filters.manufacturer || ''}
                  onChange={(value) => handleFilterChange('manufacturer', value)}
                  options={filterOptions.manufacturer || []}
                  placeholder="Search manufacturer..."
                  testId="filter-manufacturer-text"
                />
              )}

              {dataset === 'sales' && (
                <AutocompleteInput
                  field="number_of_sellers"
                  label="NUMBER OF SELLERS"
                  value={filters.number_of_sellers || ''}
                  onChange={(value) => handleFilterChange('number_of_sellers', value)}
                  options={filterOptions.number_of_sellers || []}
                  placeholder="Search number of sellers..."
                  testId="filter-number-of-sellers"
                />
              )}
            </div>
          )}
        </section>

        {/* Results Section */}
        <section className="results-section" data-testid="results-section">
          <div className="results-header">
            <h2>Results</h2>
            <span className="result-count" data-testid="result-count">
              {pagination.total.toLocaleString()} records found
            </span>
          </div>

          {loading ? (
            <div className="loading" data-testid="loading-indicator">Loading...</div>
          ) : (
            <>
              <div className="table-container">
                <table className="data-table" data-testid="data-table">
                  <thead>
                    <tr>
                      {config.columns.map(col => (
                        <th 
                          key={col} 
                          onClick={() => handleSort(col)}
                          className={sortBy === col ? `sorted ${sortOrder.toLowerCase()}` : ''}
                          data-testid={`column-header-${col}`}
                        >
                          {col.replace(/_/g, ' ').toUpperCase()}
                          {sortBy === col && (
                            <span className="sort-indicator">
                              {sortOrder === 'ASC' ? ' ▲' : ' ▼'}
                            </span>
                          )}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.map((row, idx) => (
                      <tr key={row.id || idx} data-testid={`data-row-${idx}`}>
                        {config.columns.map(col => (
                          <td key={col} title={row[col] || ''}>
                            {renderCellContent(col, row[col])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              <div className="pagination" data-testid="pagination">
                <button
                  disabled={pagination.page <= 1}
                  onClick={() => search(pagination.page - 1)}
                  data-testid="prev-page"
                >
                  Previous
                </button>
                <span className="page-info">
                  Page {pagination.page} of {pagination.totalPages}
                </span>
                <button
                  disabled={pagination.page >= pagination.totalPages}
                  onClick={() => search(pagination.page + 1)}
                  data-testid="next-page"
                >
                  Next
                </button>
              </div>
            </>
          )}
        </section>
      </main>

      {/* Footer */}
      <footer className="footer" data-testid="app-footer">
        <p>The Drug Database - FDA Orange Book Data</p>
      </footer>
    </div>
  );
}

export default App;
