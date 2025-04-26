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

            localStorage.setItem(`sorare-lineup-${username}`, JSON.stringify(config));
        }

        function loadLineupConfiguration(username) {
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
        }

        // Initialize Sortable.js
        const sortableOptions = {
            group: 'lineups',
            animation: 150,
            multiDrag: true,
            selectedClass: 'bg-info text-white',
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
                    statusDiv.className = 'db-status alert alert-success';
                    statusDiv.innerHTML = `
                        <strong>Database Connected:</strong> 
                        Game Week: ${data.game_week} | 
                        <small><strong>Last Updated:</strong> ${data.last_updated}</small>
                    `;
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

            // Show loading spinner
            document.querySelector('.loading').style.display = 'block';
            document.getElementById('loadingMessage').textContent = 'Generating lineups...';

            // Gather form data
            const formData = new FormData(lineupForm);

            // Send a POST request to generate lineups
            fetch('/generate_lineup', {
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

    function fetchWeatherReport() {
        fetch('/weather_report')
            .then(response => {
                return response.json();
            })
            .then(data => {
                const weatherReportContainer = document.getElementById('weatherReport');
                if (data.success) {
                    weatherReportContainer.innerHTML = data.weather_html;
                } else {
                    weatherReportContainer.innerHTML = `<p class="text-danger">Error: ${data.error}</p>`;
                }
            })
            .catch(error => {
                const weatherReportContainer = document.getElementById('weatherReport');
                weatherReportContainer.innerHTML = `<p class="text-danger">Error fetching weather report: ${error.message}</p>`;
            });
    }

    // --- Initialization ---
    initializeLineupManagement();
    initializeEventHandlers();
    checkDatabaseStatus();
    fetchWeatherReport();
});