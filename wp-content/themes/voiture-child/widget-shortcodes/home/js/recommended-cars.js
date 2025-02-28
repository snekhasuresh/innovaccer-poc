document.addEventListener('DOMContentLoaded', function() {
    var tabLinks = document.querySelectorAll('.custom-recommended-tab-link');
    var tabContents = document.querySelectorAll('.custom-recommended-tab-content');

    tabLinks.forEach(function(link) {
        link.addEventListener('click', function() {
            var tabId = this.getAttribute('data-tab');

            tabLinks.forEach(function(link) {
                link.classList.remove('current');
            });

            tabContents.forEach(function(content) {
                content.classList.remove('current');
            });

            this.classList.add('current');
            document.getElementById(tabId).classList.add('current');
        });
    });
});