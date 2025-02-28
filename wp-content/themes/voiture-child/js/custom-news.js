jQuery(document).ready(function ($) {
  let paged = 1;

  const scrollRightBtn = document.getElementById("scroll-right");
  const scrollLeftBtn = document.getElementById("scroll-left");
  const menu = document.getElementById("category-tabs");

  // Scroll button logic
  scrollRightBtn.addEventListener("click", () => {
    menu.scrollLeft += 99;
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
    let category_name =  $(this).data("categoryname");
    let category_slug =  convertToSlug($(this).data("categoryslug"));
    console.log(category_name, ' : ', category_slug);
    recordVirtualPageview(category_name, category_slug);

    $("#selected-category-id").val(category_id);

    loadCategoryNews(category_id);

    $(".tab-link.selected").removeClass("selected");
    $(this).addClass("selected");
  });

  const initialSelectedTab = $(".tab-link.selected").data("category");
  loadCategoryNews(initialSelectedTab);

  function loadCategoryNews(category_id) {
    $("#subcategories-container").hide();

    const urlParams = new URLSearchParams(window.location.search);
    const lang = urlParams.get('lang');

    $.ajax({
      url: ajax_data.ajax_url,
      type: "POST",
      data: {
        action: "load_subcategory_news",
        lang: lang,
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
            const subcategory_name =  $(this).data("subcategoryname");
            const category_name =  $(this).data("categoryname");
            let category_slug = convertToSlug(category_name);
            let subcategory_slug = convertToSlug(subcategory_name);
            recordVirtualPageview(category_name, category_slug, subcategory_name, subcategory_slug);
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

    const urlParams = new URLSearchParams(window.location.search);
    const lang = urlParams.get('lang');

    $.ajax({
      url: ajax_data.ajax_url,
      type: "POST",
      data: {
        action: "load_subcategory_news",
        category_id: $("#selected-category-id").val(),
        subcategory_id: subcategory_id,
        lang: lang,
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
        action: "load_subcategory_news",
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

function recordVirtualPageview(categoryName, categorySlug, subcategoryName='', subcategorySlug='') {
  console.log('recording virtual view...........');
  const homeUrl = `${window.location.origin}/`;

  window.dataLayer = window.dataLayer || []; 
  window.pageTitle = categoryName + " Article";
  if(subcategoryName) window.pageTitle = categoryName + " " + subcategoryName+ " Article"; 
  window.pageURL = homeUrl + '/news' + categorySlug + '/' + subcategorySlug;
  window.pagePath = '/news/' + categorySlug + '/' + subcategorySlug;
  window.newsCategory = categoryName;
  window.newsSubCategory = subcategoryName;
  window.dataLayer.push({
    'event': 'virtual_pageview', 
    'pageTitle': window.pageTitle, 
    'pageURL': window.pageURL, 
    'pagePath': window.pagePath, 
    'newsCategory': window.newsCategory, 
    'newsSubCategory': window.newsSubCategory
  });
}

function convertToSlug(str) {
  return str
      .toLowerCase() // Convert to lowercase
      .trim() // Remove leading and trailing spaces
      .replace(/[\s\W-]+/g, '-') // Replace spaces and non-alphanumeric characters with hyphens
      .replace(/^-+|-+$/g, ''); // Remove leading and trailing hyphens
}