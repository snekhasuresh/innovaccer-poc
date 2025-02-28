jQuery(document).ready(function ($) {
  let paged = 1;

  const scrollRightBtn = document.getElementById("scroll-right");
  const scrollLeftBtn = document.getElementById("scroll-left");
  const menu = document.getElementById("category-tabs");

  // Scroll button logic
  scrollRightBtn.addEventListener("click", () => {
    menu.scrollLeft += 100;
    updateButtonVisibility();
  });

  scrollLeftBtn.addEventListener("click", () => {
    if (menu.clientWidth === 1209 && menu.scrollLeft === 100) {
      menu.scrollLeft = 0;
    } else {
      menu.scrollLeft -= 100;
    }
    updateButtonVisibility();
  });

  function updateButtonVisibility() {
    scrollLeftBtn.classList.toggle(
      "visible",
      menu.scrollLeft > 0 || menu.clientWidth == 1257
    );
    scrollRightBtn.classList.toggle(
      "visible",
      menu.scrollWidth > menu.clientWidth + menu.scrollLeft
    );
  }

  // Tab click handler
  $(".tab-link").on("click", function () {
    paged = 1;
    var category_id = $(this).data("category");
    $("#selected-category-id").val(category_id);

    loadCategoryNews(category_id);

    $(".tab-link.selected").removeClass("selected");
    $(this).addClass("selected");
  });

  const initialSelectedTab = $(".tab-link.selected").data("category");
  loadCategoryNews(initialSelectedTab);

  function loadCategoryNews(category_id) {
    $("#subcategories-container").hide();

    $.ajax({
      url: ajax_data.ajax_url,
      type: "POST",
      data: {
        action: "load_subcategory_motor_news",
        category_id: category_id,
        paged: paged,
      },
      success: function (response) {
        var result = JSON.parse(response);
        $("#subcategories-container").html(result.subcategories);
        $("#news-articles").html(result.posts);
        if (category_id != 0) {
          $("#subcategories-container").show();
        }
        if (result.has_more) {
          $("#load-more-link").show();
        } else {
          $("#load-more-link").hide();
        }
        // Add click handler for dynamically loaded subcategories
        $(".subcategory-tab")
          .off("click")
          .on("click", function () {
            const subcategory_id = $(this).data("subcategory");
            loadSubcategoryNews(subcategory_id);

            $(".subcategory-tab.sub-active").removeClass("sub-active");
            $(this).addClass("sub-active");
          });
      },
      error: function (error) {
        $("#subcategories-container").html(
          "<p>Error loading subcategories.</p>"
        );
        $("#news-articles").html("<p>Error loading news articles.</p>");
      },
    });
  }

  function loadSubcategoryNews(subcategory_id) {
    paged = 1;

    $.ajax({
      url: ajax_data.ajax_url,
      type: "POST",
      data: {
        action: "load_subcategory_motor_news",
        category_id: $("#selected-category-id").val(),
        subcategory_id: subcategory_id,
        paged: paged,
      },
      success: function (response) {
        const result = JSON.parse(response);
        $("#news-articles").html(result.posts);
        if (result.has_more) {
          $("#load-more-link").show();
        } else {
          $("#load-more-link").hide();
        }
      },
      error: function () {
        $("#news-articles").html("<p>Error loading news articles.</p>");
      },
    });
  }

  // Function to load news articles
  function loadNews() {
    paged++;
    const category_id = $("#selected-category-id").val();

    // Load more news articles
    $.ajax({
      url: ajax_data.ajax_url,
      type: "POST",
      data: {
        action: "load_subcategory_motor_news",
        category_id: category_id,
        paged: paged,
      },
      success: function (response) {
        const result = JSON.parse(response);

        $("#news-articles").append(result.posts);

        if (result.has_more) {
          $("#load-more-link").show();
        } else {
          $("#load-more-link").hide();
        }
      },
      error: function (error) {
        $("#news-articles").append("<p>Error loading more articles.</p>");
      },
    });
  }

  // Event listener for the "View More" link
  $("#load-more-link").on("click", function (e) {
    e.preventDefault();
    loadNews();
  });

  updateButtonVisibility();
  window.addEventListener("resize", updateButtonVisibility);
});
