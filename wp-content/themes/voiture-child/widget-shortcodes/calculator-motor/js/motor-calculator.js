jQuery(document).ready(function ($) {
    function initializeDropdowns() {
        // Clear previous event handlers to avoid duplicates
        $(".brand-data").off("click");
        $(".model-item").off("click");
        $(".variant-item").off("click");
        $("#select-cars").off("click");

        // Clear previous data from dropdown menus
        $(".model-menu").empty();
        $(".variant-menu").empty();
        $("#select-cars").text("Select Car");

        // Handle clicks on brand items
        $(document).on("click", ".brand-data", function (e) {
            e.preventDefault();
            var brandId = $(this).data("brand-id");

            // Clear model and variant menus
            $(".model-menu").hide();
            $(".variant-menu").hide();

            $(".brand-data").removeClass("selected");
            $(this).addClass("selected");

            fetchModels(brandId);
        });

        // Handle clicks on model items
        $(document).on("click", ".model-item", function (e) {
            e.preventDefault();
            var modelId = $(this).data("model-id");

            // Clear variant menu
            $(".variant-menu").empty();

            $(".model-item").removeClass("selected");
            $(this).addClass("selected");

            // Fetch and display variants for the selected model
            fetchVariants(modelId);
        });

        // Handle clicks on variant items
        $(document).on("click", ".variant-item", function (e) {
            e.preventDefault();
            e.stopPropagation();
            var variantText = $(this).text();
            var variantId = $(this).data("variant-id");

            $(".variant-item").removeClass("selected");
            $(this).addClass("selected");

            $("#select-cars").text(variantText);
            $(".dropdown-menu").hide();

            // Fetch variant data
            fetchVariantData(variantId);
        });

        $(document).on("click.dropdownHandler", function (e) {
            if (!$(e.target).closest(".dropdown").length) {
                $(".dropdown-menu").hide();
            }
        });

        // Toggle dropdown visibility
        $("#select-cars").on("click", function (e) {
            e.preventDefault();
            var $dropdownMenu = $(this).next(".dropdown-menu");
            $(".dropdown-menu").not($dropdownMenu).hide();
            $dropdownMenu.toggle();
        });
    }

    // Function to fetch models based on selected brand
    function fetchModels(brandId) {
        var loadingIndicator = '<span class="loader"><i class="fas fa-spinner fa-spin"></i></span>';
        $(".brand-data[data-brand-id='" + brandId + "'] .right-arrow").replaceWith(loadingIndicator);

        $.ajax({
            url: ajax_data.ajax_url,
            type: "POST",
            data: {
                action: "fetch_motor_models",
                brand_id: brandId,
            },
            dataType: "json",
            success: function (response) {
                $(".brand-data[data-brand-id='" + brandId + "'] .loader").replaceWith('<span class="right-arrow">&#10095;</span>');

                var modelMenu = $(".model-menu");
                modelMenu.empty();

                if (response.success) {
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
    function fetchVariants(modelId) {
        var loadingIndicator = '<span class="loader"><i class="fas fa-spinner fa-spin"></i></span>';
        $(".model-item[data-model-id='" + modelId + "'] .right-arrow").replaceWith(loadingIndicator);

        $.ajax({
            url: ajax_data.ajax_url,
            type: "POST",
            data: {
                action: "fetch_motor_variants",
                model_id: modelId,
            },
            dataType: "json",
            success: function (response) {
                $(".model-item[data-model-id='" + modelId + "'] .loader").replaceWith('<span class="right-arrow">&#10095;</span>');
                var variantMenu = $(".variant-menu");
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
                    variantMenu.append('<li class="variant-item">No variants available</li>');
                    variantMenu.show();
                }
            },
            error: function (xhr, status, error) {
                console.error("AJAX Error:", error);
            },
        });
    }

    // Function to fetch variant data and update input fields
    function fetchVariantData(variantId) {
        $.ajax({
            url: ajax_data.ajax_url,
            type: "POST",
            data: {
                action: "fetch_motor_variant_data",
                variant_id: variantId,
            },
            dataType: "json",
            success: function (response) {
                if (response.success) {
                    $("#fuel-consumption").val(response.data.fuel_consumption).trigger("input");
                    $("#car-price").val(response.data.car_price);
                    $("#road-engine-capacity").val(response.data.capacity);
                    $("#insurance-car-price").val(response.data.car_price);
                } else {
                    console.error("Error fetching variant data:", response.data.message);
                }
            },
            error: function (xhr, status, error) {
                console.error("AJAX Error:", error);
            },
        });
    }

    // Initialize dropdowns on page load
    initializeDropdowns();
});
