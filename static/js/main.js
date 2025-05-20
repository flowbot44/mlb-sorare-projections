document.addEventListener('DOMContentLoaded', function () {
    // --- Lineup Management ---
    function initializeLineupManagement() {
        const active = document.getElementById('activeLineup');
        const inactive = document.getElementById('inactiveLineup');
        const lineupInput = document.getElementById('lineup_order');
        const usernameInput = document.getElementById('username');
        let hasManualChanges = false;

        function updateLineupOrder(isManual = true) {
            const items = active.querySelectorAll('li');
            const lineup = Array.from(items).map(item => item.textContent.trim());
            lineupInput.value = lineup.join(', ');

            if (isManual) {
                hasManualChanges = true;
                saveLineupConfiguration();
            }
        }

        function saveLineupConfiguration() {
            const username = usernameInput.value.trim();
            if (!username) return;
        
            const activeItems = Array.from(active.querySelectorAll('li')).map(item => item.textContent.trim());
            const inactiveItems = Array.from(inactive.querySelectorAll('li')).map(item => item.textContent.trim());
        
            const config = {
                active: activeItems,
                inactive: inactiveItems,
                timestamp: Date.now()
            };
        
            try {
                localStorage.setItem(`sorare-lineup-${username}`, JSON.stringify(config));
            } catch (e) {
                console.warn("Unable to save lineup to localStorage:", e);
                // Optionally show a message to the user
            }
        }

        function loadLineupConfiguration(username) {
            try {
                const savedConfig = localStorage.getItem(`sorare-lineup-${username}`);
                if (!savedConfig) return;
        
                const config = JSON.parse(savedConfig);

                // Reset lineups
                while (active.children.length > 0) {
                    inactive.appendChild(active.children[0]);
                }

                // Restore active lineup
                config.active.forEach(itemText => {
                    const matchingItem = Array.from(inactive.querySelectorAll('li')).find(
                        li => li.textContent.trim() === itemText
                    );
                    if (matchingItem) {
                        active.appendChild(matchingItem);
                    }
                });

                updateLineupOrder(false);
            } catch (e) {
                console.warn("Unable to load lineup from localStorage:", e);
            }
        }

        // Initialize Sortable.js
        const sortableOptions = {
            group: 'lineups',
            animation: 150,
            multiDrag: true,
            selectedClass: 'selected-item',
            onSort: () => updateLineupOrder(true),
            onAdd: () => updateLineupOrder(true),
            onRemove: () => updateLineupOrder(true)
        };

        new Sortable(active, sortableOptions);
        new Sortable(inactive, sortableOptions);

        // Load last username and lineup
        const lastUsername = localStorage.getItem('sorare-last-username');
        if (lastUsername) {
            usernameInput.value = lastUsername;
            loadLineupConfiguration(lastUsername);
        }

        // Handle username changes
        usernameInput.addEventListener('input', debounce(() => {
            const username = usernameInput.value.trim();
            if (username) {
                localStorage.setItem('sorare-last-username', username);
                loadLineupConfiguration(username);
            }
        }, 1000));
    }

    // --- Utility Functions ---
    function debounce(func, delay) {
        let timer;
        return function (...args) {
            clearTimeout(timer);
            timer = setTimeout(() => func.apply(this, args), delay);
        };
    }

    function showMessage(message, type = 'info') {
        const existingMessage = document.querySelector('.temp-message');
        if (existingMessage) existingMessage.remove();

        const messageDiv = document.createElement('div');
        messageDiv.className = `alert alert-${type} mt-2 temp-message`;
        messageDiv.textContent = message;

        const formContainer = document.querySelector('.form-container');
        formContainer.insertAdjacentElement('afterend', messageDiv);

        setTimeout(() => messageDiv.remove(), 3000);
    }

    // --- Database Status ---
    function checkDatabaseStatus() {
        fetch('/check_db')
            .then(response => response.json())
            .then(data => {
                const statusDiv = document.querySelector('.db-status');
                const generateBtn = document.getElementById('generateBtn');
                const dbInitContainer = document.getElementById('dbInitContainer');

                if (data.status === 'connected') {
                    if (data.projections_exist) {
                        // Database connected and projections exist for current game week
                        statusDiv.className = 'db-status alert alert-success';
                        statusDiv.innerHTML = `
                            <strong>Database Connected:</strong> 
                            Game Week: ${data.game_week} | 
                            <small><strong>Last Updated:</strong> ${data.last_updated}</small>
                        `;
                    } else {
                        // Database connected but no projections for current game week
                        statusDiv.className = 'db-status alert alert-warning';
                        statusDiv.innerHTML = `
                            <strong>New Game Week Detected:</strong> 
                            Game Week: ${data.game_week} | 
                            <small>No projections for this week yet. Update required.</small>
                        `;
                    }
                    generateBtn.disabled = false;
                    dbInitContainer.style.display = 'none';
                } else if (data.status === 'missing') {
                    statusDiv.className = 'db-status alert alert-warning';
                    statusDiv.innerHTML = `<strong>Database Not Ready:</strong> ${data.message}`;
                    generateBtn.disabled = true;
                    dbInitContainer.style.display = 'block';
                } else {
                    statusDiv.className = 'db-status alert alert-danger';
                    statusDiv.textContent = `Database Error: ${data.message}`;
                    generateBtn.disabled = true;
                }
            })
            .catch(error => {
                const statusDiv = document.querySelector('.db-status');
                statusDiv.className = 'db-status alert alert-danger';
                statusDiv.textContent = `Error checking database: ${error}`;
                document.getElementById('generateBtn').disabled = true;
            });
    }

    // --- Event Handlers ---
    function initializeEventHandlers() {
        // Handle form submission for "Generate Lineups"
        const lineupForm = document.getElementById('lineupForm');
        lineupForm.addEventListener('submit', (event) => {
            event.preventDefault(); // Prevent the default form submission behavior

            // Get form fields
            const username = document.getElementById('username').value.trim();
            const rareEnergy = document.getElementById('rare_energy').value.trim();
            const limitedEnergy = document.getElementById('limited_energy').value.trim();
            const boost2025 = document.getElementById('boost_2025').value.trim();
            const stackBoost = document.getElementById('stack_boost').value.trim();

            // Validate fields
            if (!username) {
                alert('Sorare Username is required.');
                return;
            }
            if (!rareEnergy || isNaN(rareEnergy)) {
                alert('Rare Energy is required and must be a number.');
                return;
            }
            if (!limitedEnergy || isNaN(limitedEnergy)) {
                alert('Limited Energy is required and must be a number.');
                return;
            }
            if (!boost2025 || isNaN(boost2025)) {
                alert('2025 Card Boost is required and must be a number.');
                return;
            }
            if (!stackBoost || isNaN(stackBoost)) {
                alert('Stack Boost is required and must be a number.');
                return;
            }

            // Show loading spinner
            document.querySelector('.loading').style.display = 'block';
            document.getElementById('loadingMessage').textContent = 'Generating lineups...';

            // Gather form data
            const formData = new FormData(lineupForm);

            // Send a POST request to generate lineups
            fetch('/generate', {
                method: 'POST',
                body: formData,
            })
                .then((response) => response.json())
                .then((data) => {
                    document.querySelector('.loading').style.display = 'none';

                    if (data.error) {
                        alert(`Error: ${data.error}`);
                    } else {
                        // Display the generated lineups in the results container
                        const resultsContainer = document.getElementById('lineupResults');
                        resultsContainer.innerHTML = data.lineup_html; // Assuming the server returns HTML for the lineups
                        document.querySelector('.results-container').style.display = 'block';
                        
                        // Update the displayed username in the results
                        document.getElementById('results-username').textContent = document.getElementById('username').value;
                    }
                })
                .catch((error) => {
                    document.querySelector('.loading').style.display = 'none';
                    alert(`Error: ${error.message}`);
                });
        });

        // Handle "Update Injuries & Projections" button
        document.getElementById('updateDataBtn').addEventListener('click', () => {
            if (!confirm('This will update injuries and projections. Continue?')) return;

            document.querySelector('.loading').style.display = 'block';
            document.getElementById('loadingMessage').textContent = 'Updating injuries and projections...';

            fetch('/update_data', { method: 'POST' })
                .then((response) => response.json())
                .then((data) => {
                    document.querySelector('.loading').style.display = 'none';
                    if (data.error) {
                        alert(`Error: ${data.error}`);
                    } else {
                        alert(data.message);
                        window.location.reload();
                    }
                })
                .catch((error) => {
                    document.querySelector('.loading').style.display = 'none';
                    alert(`Error: ${error.message}`);
                });
        });


        document.getElementById('fullUpdateBtn').addEventListener('click', runFullUpdate);
    }

    function runFullUpdate() {
        document.querySelector('.loading').style.display = 'block';
        document.getElementById('loadingMessage').textContent = 'Initializing database... This will take several minutes.';

        fetch('/run_full_update', { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                document.querySelector('.loading').style.display = 'none';
                if (data.error) {
                    alert(data.error);
                } else {
                    alert(data.message);
                    window.location.reload();
                }
            })
            .catch(error => {
                document.querySelector('.loading').style.display = 'none';
                alert(`Error: ${error.message}`);
            });
    }

    // --- Initialization ---
    initializeLineupManagement();
    initializeEventHandlers();
    checkDatabaseStatus();
    updateWeatherReportButtons();
    initializeIgnoreGamesClearButton();
    
    // Set up event delegation for the ignore game buttons that will be added dynamically
    document.getElementById('weatherReport').addEventListener('click', function(e) {
        if (e.target && e.target.classList.contains('ignore-game-btn')) {
            const gameId = e.target.getAttribute('data-game-id');
            addGameToIgnoreList(gameId);
            
            // Update button state
            e.target.classList.remove('btn-warning');
            e.target.classList.add('btn-secondary');
            e.target.textContent = 'Added to Ignore List';
            e.target.disabled = true;
            
            // Highlight the row to indicate it's been added
            const row = e.target.closest('tr');
            row.classList.add('table-secondary');
        }
    });

    // Set up the refresh weather button event listener
    const refreshWeatherBtn = document.getElementById('refreshWeatherBtn');
    if (refreshWeatherBtn) {
        refreshWeatherBtn.addEventListener('click', function() {
            this.disabled = true;
            this.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refreshing...';
            
            loadWeatherReport().then(() => {
                this.disabled = false;
                this.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refresh';
            });
        });
    }
});

/**
 * Load the weather report from the server
 * Made global so it can be called from anywhere
 * @returns {Promise} A promise that resolves when the weather report is loaded
 */
function loadWeatherReport() {
    return fetch('/weather_report')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                document.getElementById('weatherReport').innerHTML = data.weather_html;
                
                // After loading weather report, check which games are already in ignore list
                updateWeatherReportButtons();
            } else {
                document.getElementById('weatherReport').innerHTML = '<div class="alert alert-danger">Failed to load weather report</div>';
            }
            return data;
        })
        .catch(error => {
            document.getElementById('weatherReport').innerHTML = `<div class="alert alert-danger">Error: ${error.message}</div>`;
            throw error;
        });
}

/**
 * Add a game ID to the ignore games input field
 */
function addGameToIgnoreList(gameId) {
    const ignoreGamesInput = document.getElementById('ignore_games');
    let currentIds = ignoreGamesInput.value.split(',').map(id => id.trim()).filter(id => id);
    
    // Only add the ID if it's not already in the list
    if (!currentIds.includes(gameId)) {
        currentIds.push(gameId);
        ignoreGamesInput.value = currentIds.join(', ');
        
        // Show feedback to the user
        const alertHtml = `
            <div class="alert alert-success alert-dismissible fade show mt-2" role="alert">
                Game ID ${gameId} added to ignore list
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
        `;
        
        const alertContainer = document.createElement('div');
        alertContainer.innerHTML = alertHtml;
        document.querySelector('.weather-report-container').appendChild(alertContainer.firstElementChild);
        
        // Auto dismiss after 3 seconds
        setTimeout(() => {
            const alert = document.querySelector('.weather-report-container .alert');
            if (alert) {
                alert.remove();
            }
        }, 3000);
    }
}

/**
 * Update the ignore game buttons based on what's already in the ignore list
 */
function updateWeatherReportButtons() {
    const ignoreGamesInput = document.getElementById('ignore_games');
    const currentIds = ignoreGamesInput.value.split(',').map(id => id.trim()).filter(id => id);
    
    // Find all the ignore game buttons in the weather report
    const ignoreButtons = document.querySelectorAll('.ignore-game-btn');
    
    ignoreButtons.forEach(button => {
        const gameId = button.getAttribute('data-game-id');
        
        if (currentIds.includes(gameId)) {
            // This game is already being ignored, update the button
            button.classList.remove('btn-warning');
            button.classList.add('btn-secondary');
            button.textContent = 'Added to Ignore List';
            button.disabled = true;
            
            // Also update the row
            const row = button.closest('tr');
            if (row) {
                row.classList.add('table-secondary');
            }
        }
    });
}

/**
 * Add a clear button to the ignore games list
 */
function initializeIgnoreGamesClearButton() {
    const ignoreGamesInput = document.getElementById('ignore_games');
    if (!ignoreGamesInput) return;
    
    // Find the clear button, whether it was created dynamically or exists in HTML
    let clearButton = document.getElementById('clearIgnoreGamesBtn');
    
    // If button doesn't exist, create it
    if (!clearButton) {
        const inputGroup = ignoreGamesInput.parentElement;
        
        // Create the clear button
        const clearButtonHtml = `
            <button class="btn btn-outline-secondary" type="button" id="clearIgnoreGamesBtn" title="Clear all ignored games">
                <i class="bi bi-x-circle"></i> Clear
            </button>
        `;
        
        // Wrap the input in an input group if it's not already
        if (!inputGroup.classList.contains('input-group')) {
            const wrapper = document.createElement('div');
            wrapper.className = 'input-group';
            ignoreGamesInput.parentNode.insertBefore(wrapper, ignoreGamesInput);
            wrapper.appendChild(ignoreGamesInput);
            wrapper.insertAdjacentHTML('beforeend', clearButtonHtml);
        } else {
            inputGroup.insertAdjacentHTML('beforeend', clearButtonHtml);
        }
        
        clearButton = document.getElementById('clearIgnoreGamesBtn');
    }
    
    // Add the event listener - this happens whether the button was just created or already existed
    clearButton.addEventListener('click', function() {
        ignoreGamesInput.value = '';
        
        // Update the weather report buttons
        updateWeatherReportButtons();
        
        // Show feedback
        const alertHtml = `
            <div class="alert alert-info alert-dismissible fade show mt-2" role="alert">
                Ignore games list cleared
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
        `;
        
        const alertContainer = document.createElement('div');
        alertContainer.innerHTML = alertHtml;
        document.querySelector('.form-container').appendChild(alertContainer.firstElementChild);
        
        // Auto dismiss after 3 seconds
        setTimeout(() => {
            const alert = document.querySelector('.form-container .alert');
            if (alert) {
                alert.remove();
            }
        }, 3000);
    });
}