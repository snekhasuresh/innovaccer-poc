document.addEventListener("DOMContentLoaded", function () {
    const dropdownToggle = document.querySelector(".user-dropdown .dropdown-toggle");
    const dropdownMenu = document.querySelector(".user-dropdown .dropdown-menu");

    if (dropdownToggle && dropdownMenu) {
        dropdownToggle.addEventListener("click", function (e) {
            e.stopPropagation();
            dropdownMenu.classList.toggle("show");
        });

        // Close dropdown when clicking outside
        document.addEventListener("click", function () {
            if (dropdownMenu.classList.contains("show")) {
                dropdownMenu.classList.remove("show");
            }
        });
    }
});
