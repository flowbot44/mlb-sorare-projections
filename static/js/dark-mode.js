document.addEventListener('DOMContentLoaded', function () {
    function initializeDarkMode() {
        const toggle = document.getElementById('darkModeToggle');
        const body = document.body;

        // Apply stored dark mode
        if (localStorage.getItem('darkMode') === 'true') {
            body.classList.add('dark-mode');
            if (toggle) toggle.checked = true;
        }

        if (toggle) {
            toggle.addEventListener('change', function () {
                const isDarkMode = toggle.checked;
                body.classList.toggle('dark-mode', isDarkMode);
                localStorage.setItem('darkMode', isDarkMode.toString());
            });
        }
    }

    initializeDarkMode();
});