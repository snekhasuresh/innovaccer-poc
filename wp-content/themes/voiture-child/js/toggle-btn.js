jQuery(document).ready(function ($) {
  function toggleSections() {
    if ($(".custom_toggle_switch ul.nav-tabs li:first-child").hasClass("active")) {
      $(".car-section").show();
      $(".motorcycle-section").hide();
      $("#car-header-section").show();
      $("#motorcycle-header-section").hide();
    } else {
      $("#car-header-section").hide();
      $("#motorcycle-header-section").show();
      $(".car-section").hide();
      $(".motorcycle-section").show();
    }
  }

  $(window).on("load", function () {
    toggleSections();
  });

  var observer = new MutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
      if (mutation.attributeName === "class") {
        toggleSections();
      }
    });
  });
  $(".custom_toggle_switch ul.nav-tabs li").each(function () {
    observer.observe(this, { attributes: true });
  });
});
