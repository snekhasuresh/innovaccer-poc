jQuery(document).ready(function ($) {
  function initializeDropdowns($tab) {
    // Clear previous event handlers to avoid duplicates
    $tab.find(".brand-data").off("click");
    $tab.find(".model-item").off("click");
    $tab.find(".variant-item").off("click");
    $tab.find("#select-cars").off("click");

    // Clear previous data from dropdown menus
    $tab.find(".model-menu").empty();
    $tab.find(".variant-menu").empty();
    $tab.find("#select-cars").text("Chọn xe của bạn");

    // Handle clicks on brand items
    $tab.on("click", ".brand-data", function (e) {
      e.preventDefault();
      var brandId = $(this).data("brand-id");

      // Clear model and variant menus
      $tab.find(".model-menu").hide();
      $tab.find(".variant-menu").hide();

      $tab.find(".brand-data").removeClass("selected");
      $(this).addClass("selected");

      fetchModels(brandId, $tab);
    });

    // Handle clicks on model items
    $tab.on("click", ".model-item", function (e) {
      e.preventDefault();
      var modelId = $(this).data("model-id");

      // Clear variant menu
      $tab.find(".variant-menu").empty();

      $tab.find(".model-item").removeClass("selected");
      $(this).addClass("selected");

      // Fetch and display variants for the selected model
      fetchVariants(modelId, $tab);
    });

    // Handle clicks on variant items
    $tab.on("click", ".variant-item", function (e) {
      e.preventDefault();
      e.stopPropagation();
      var variantText = $(this).text();
      var variantId = $(this).data("variant-id");

      $tab.find(".variant-item").removeClass("selected");
      $(this).addClass("selected");

      $tab.find("#select-cars").text(variantText);
      $tab.find(".dropdown-menu").hide();

      // Fetch variant data
      fetchVariantData(variantId, $tab);
    });

    $(document)
      .off("click.dropdownHandler")
      .on("click.dropdownHandler", function (e) {
        if (!$(e.target).closest($tab.find(".dropdown")).length) {
          $tab.find(".dropdown-menu").hide();
        }
      });
    // Toggle dropdown visibility
    $tab.find("#select-cars").on("click", function (e) {
      e.preventDefault();
      var $dropdownMenu = $(this).next(".dropdown-menu");
      $(".dropdown-menu").not($dropdownMenu).hide();
      $dropdownMenu.toggle();
    });
  }

  // Function to fetch models based on selected brand
  function fetchModels(brandId, $tab) {
    var loadingIndicator =
      '<span class="loader"><i class="fas fa-spinner fa-spin"></i></span>';
    $tab
      .find(".brand-data[data-brand-id='" + brandId + "'] .right-arrow")
      .replaceWith(loadingIndicator);

    $.ajax({
      url: ajax_data.ajax_url,
      type: "POST",
      data: {
        action: "fetch_car_models",
        brand_id: brandId,
      },
      dataType: "json",
      success: function (response) {
        $tab
          .find(".brand-data[data-brand-id='" + brandId + "'] .loader")
          .replaceWith('<span class="right-arrow">&#10095;</span>');

        var modelMenu = $tab.find(".model-menu");
        modelMenu.empty();

        if (response.success) {
          // Populate the model menu using the provided response data structure
          response.data.forEach(function (model) {
            modelMenu.append(
              '<li class="model-item" data-model-id="' +
                model.ID +
                '"><a href="#">' +
                model.post_title +
                "</a><div><span class='right-arrow'>&#10095;</span></div></li>"
            );
          });

          // Show the model menu after fetching data
          modelMenu.show();
        } else {
          modelMenu.append('<li class="model-item">No models available</li>');
          modelMenu.show();
        }
      },
      error: function (xhr, status, error) {
        console.error("AJAX Error:", error);
      },
    });
  }

  // Function to fetch variants based on selected model
  function fetchVariants(modelId, $tab) {
    var loadingIndicator =
      '<span class="loader"><i class="fas fa-spinner fa-spin"></i></span>';
    $tab
      .find(".model-item[data-model-id='" + modelId + "'] .right-arrow")
      .replaceWith(loadingIndicator); // Show loader

    $.ajax({
      url: ajax_data.ajax_url,
      type: "POST",
      data: {
        action: "fetch_car_variants",
        model_id: modelId,
      },
      dataType: "json",
      success: function (response) {
        $tab
          .find(".model-item[data-model-id='" + modelId + "'] .loader")
          .replaceWith('<span class="right-arrow" >&#10095;</span>');
        var variantMenu = $tab.find(".variant-menu");
        variantMenu.empty();
        if (response.success) {
          response.data.forEach(function (variant) {
            variantMenu.append(
              '<li class="variant-item" data-variant-id="' +
                variant.ID +
                '"><a href="#">' +
                variant.post_title +
                "</a></li>"
            );
          });

          // Show the variant menu after fetching data
          variantMenu.show();
        } else {
          variantMenu.append(
            '<li class="variant-item">No variants available</li>'
          );
          variantMenu.show();
        }
      },
      error: function (xhr, status, error) {
        console.error("AJAX Error:", error);
      },
    });
  }

  // Function to fetch variant data and update input fields
  function fetchVariantData(variantId, $tab) {
    $.ajax({
      url: ajax_data.ajax_url,
      type: "POST",
      data: {
        action: "fetch_car_variant_data",
        variant_id: variantId,
      },
      dataType: "json",
      success: function (response) {
        if (response.success) {
          $tab
            .find("#fuel-consumption")
            .val(response.data.fuel_consumption)
            .trigger("input");
          $tab.find("#car-price").val(response.data.car_price);
          $tab.find("#road-engine-capacity").val(response.data.capacity);
          $tab.find("#insurance-car-price").val(response.data.car_price);
        } else {
          console.error("Error fetching variant data:", response.data.message);
        }
      },
      error: function (xhr, status, error) {
        console.error("AJAX Error:", error);
      },
    });
  }

  // Initialize dropdowns for the currently active tab on page load
  const $currentTab = $(".tab-content.current");
  if ($currentTab.length) {
    initializeDropdowns($currentTab);
  }

  // Tab switch handler
  $(".tab-head").on("click", function () {
    const tabId = $(this).data("tab");
    const $selectedTab = $("#" + tabId);

    // Hide all tab contents and show the selected one
    $(".tab-content").removeClass("current");
    $selectedTab.addClass("current");

    // Re-initialize dropdowns for the newly selected tab
    initializeDropdowns($selectedTab);
  });
});
