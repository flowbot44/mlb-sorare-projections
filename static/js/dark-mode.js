// static/js/dark-mode.js

document.addEventListener('DOMContentLoaded', function() {
    const darkModeToggle = document.getElementById('darkModeToggle');
    
    // Check for saved dark mode preference
    const isDarkMode = localStorage.getItem('darkMode') === 'true';
    
    // Apply dark mode if saved preference exists
    if (isDarkMode) {
        document.body.classList.add('dark-mode');
        darkModeToggle.checked = true;
    }
    
    // Toggle dark mode when checkbox is clicked
    darkModeToggle.addEventListener('change', function() {
        if (this.checked) {
            document.body.classList.add('dark-mode');
            localStorage.setItem('darkMode', 'true');
        } else {
            document.body.classList.remove('dark-mode');
            localStorage.setItem('darkMode', 'false');
        }
        
        // Trigger an event for other scripts to react to dark mode changes
        const darkModeEvent = new CustomEvent('darkModeChanged', {
            detail: { isDarkMode: this.checked }
        });
        document.dispatchEvent(darkModeEvent);
    });
    
    // Make sure all toggles on the page stay in sync
    document.addEventListener('change', function(event) {
        if (event.target.id === 'darkModeToggle') {
            const allToggles = document.querySelectorAll('#darkModeToggle');
            
            allToggles.forEach(toggle => {
                if (toggle !== event.target) {
                    toggle.checked = event.target.checked;
                }
            });
        }
    });
});