document.addEventListener("DOMContentLoaded", function () {
  const tabs = document.querySelectorAll(".bike-tabs .tabs a");
  const panes = document.querySelectorAll(".bike-tabs .tab-pane");

  tabs.forEach(function (tab) {
    tab.addEventListener("click", function (event) {
      event.preventDefault();
      // Remove active class from all tabs and panes
      tabs.forEach((t) => t.classList.remove("active"));
      panes.forEach((p) => p.classList.remove("active"));
      // Add active class to the clicked tab and corresponding pane
      tab.classList.add("active");
      const targetPane = document.querySelector(tab.getAttribute("href"));
      targetPane.classList.add("active");
    });
  });

  // Set the first tab and pane as active by default
  tabs[0].classList.add("active");
  panes[0].classList.add("active");
});
