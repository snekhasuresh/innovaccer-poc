// Add console log to verify script loading
console.log('Language switcher script loaded');

// Cookie management functions
function setCookie(name, value, days) {
    var expires = "";
    if (days) {
        var date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        expires = "; expires=" + date.toUTCString();
    }
    document.cookie = name + "=" + (value || "") + expires + "; path=/";
}

function getCookie(name) {
    var nameEQ = name + "=";
    var ca = document.cookie.split(";");
    for (var i = 0; i < ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) == " ") c = c.substring(1, c.length);
        if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length, c.length);
    }
    return null;
}

// Initialize language switcher functionality
function initLanguageSwitcher() {
    console.log('Initializing language switcher');
    const langMap = {
        'bm': 'Bahasa Malaysia',
        'zh': '中文', // Chinese
        null: 'English'
    };
    // Default language mapping
    const languageMapping = {
        "English": "en",
        "中文": "my-zh", // Chinese
        "Bahasa Malaysia": "my-my"
    };
    
    let lang;
    // Get last segment of URL
    const url = window.location.href;
    let segments = url.split('/');
    segments = segments.filter(segment => segment !== '');
    const lastSegment = segments[segments.length - 1];
    if (lastSegment === 'bm' || lastSegment === 'zh') {
        lang = lastSegment;
    }

    const languageSwitcher = document.getElementById('language-switcher');
    console.log('Language switcher element:', languageSwitcher);

    if (!languageSwitcher) {
        console.log('Language switcher element not found');
        return;
    }

    // Check for saved language in cookie first
    const savedLang = getCookie("preferred_language");
    console.log('Saved language from cookie:', savedLang);

    if (savedLang) {
        for (let option of languageSwitcher.options) {
            if (option.text === savedLang) {
                option.selected = true;
                break;
            }
        }
    } else {
        // If no cookie, default to English (or any default you prefer)
        //languageSwitcher.value = 'English'; // Set default option to English if no cookie is found

        // If no cookie, use URL-based language
        const selectedLanguage = langMap[lang];
        console.log('Selected language from URL:', selectedLanguage);

        if (selectedLanguage) {
            for (let option of languageSwitcher.options) {
                if (option.text === selectedLanguage) {
                    option.selected = true;
                    break;
                }
            }
        }
    }

    // Add change event listener to the language switcher
    languageSwitcher.addEventListener('change', function (event) {

        const selectedText = this.options[this.selectedIndex].text; // Get the selected language text
        const selectedUrl = this.value;

        // Set cookies for language
        setCookie("user_language", languageMapping[selectedText], 30); // Store for 30 days
        setCookie("preferred_language", selectedText, 30); // Store for 30 days

        console.log(`User language cookie set: ${languageMapping[selectedText]}`);
        console.log(`Preferred language cookie set: ${selectedText}`);

        // Conditional handling for different pages
        if (window.location.pathname === "/" || window.location.pathname === "/home") {
            // For home page, reload
            console.log("Reloading home page for language change...");
            window.location.reload();
        } else if (window.location.pathname.includes('/news') || "/bm" || "/zh") {
            // For news page, redirect to selected URL
            console.log("Redirecting to selected URL for news page...");
            window.location.href = selectedUrl;
        } else {
            // Default behavior: reload
            console.log("Default behavior: reloading page...");
            window.location.reload();
        }
    });
}

// Add both DOMContentLoaded and load event listeners for redundancy
document.addEventListener("DOMContentLoaded", initLanguageSwitcher);
window.addEventListener("load", initLanguageSwitcher);
