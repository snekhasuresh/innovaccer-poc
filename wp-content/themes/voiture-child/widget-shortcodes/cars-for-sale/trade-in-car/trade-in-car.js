document.querySelector('.trade-in-button').addEventListener('click', function () {
    document.getElementById('carPopup').style.display = 'flex';
});

document.addEventListener('DOMContentLoaded', function () {
    function showCarPopup() {
        document.getElementById('carPopup').style.display = 'flex';
    }

    function hideCarPopup() {
        document.getElementById('carPopup').style.display = 'none';
    }

    // Add click event listeners
    document.querySelector('.close-popup').addEventListener('click', hideCarPopup);

    // Close popup when clicking outside
    document.getElementById('carPopup').addEventListener('click', function (e) {
        if (e.target === this) {
            hideCarPopup();
        }
    });
});
