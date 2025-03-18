<?php

function enqueue_find_new_motorcycle_css()
{
    wp_enqueue_style('find-new-cars-style', get_stylesheet_directory_uri() . '/widget-shortcodes/new-cars/css/find-new-cars.css', array(), '1.0', 'all');
}
// add_action('wp_enqueue_scripts', 'enqueue_find_new_cars_css');

function popular_bike_in_new_bikes($atts)
{
    enqueue_find_new_motorcycle_css();
    $atts = shortcode_atts(
        array(
            'brand_id' => 0,
        ),
        $atts,
        'popular_bike_in_new_bikes'
    );

    $brand_id = $atts['brand_id'];

    $popular_cars_in_malaysia = array();
    if ($brand_id === 0) {
        $popular_bikes = get_popular_bikes_data();
        $latest_bikes = get_latest_bikes_data();

        $popular_cars_in_malaysia['Phổ biến'] = $popular_bikes;
        $popular_cars_in_malaysia['Mới nhất'] = $latest_bikes;
    } else {
        $grouped_cars = get_grouped_by_type_motors_data($brand_id); //fetch_grouped_by_type_cars_data_from_db();
        $grouped_cars = array_map(function ($cars) {
            return array_map(function ($car) {
                return (array) $car;
            }, $cars);
        }, $grouped_cars);
        $popular_cars_in_malaysia = array_merge($popular_cars_in_malaysia, $grouped_cars);
    }

    ob_start();
$translate = [
    
'bike' => 'Bảng Giá Xe Máy Mới Phổ Biến Tại Việt Nam',

];



?>
    <div class="fruit-tabs" . $brand_id>
        <h2 class="wa-title-text"><?php echo $translate['bike']; ?></h2>
        <ul class="tabs">
            <div class="tab">
                <?php if (!empty($popular_cars_in_malaysia)) : ?>
                    <?php $first_tab = true; // Initialize a flag to track the first tab 
                    ?>
                    <?php foreach ($popular_cars_in_malaysia as $key => $value) : ?>
                        <li>
                            <a
                                href="#<?php echo str_replace(' ', '_', $key); ?>-content"
                                onclick="changeMotorTab('<?php echo str_replace(' ', '_', $key); ?>-content')"
                                class="tabs-link <?php echo $first_tab ? 'active' : ''; ?>">
                                <?php echo $key; ?>
                            </a>
                        </li>
                        <?php $first_tab = false; // After the first iteration, set this to false 
                        ?>
                    <?php endforeach; ?>
                <?php endif; ?>
            </div>
        </ul>
        <script>
            function changeMotorTab(id) {
                event.preventDefault();

                const tabs2 = document.querySelectorAll('.fruit-tabs .tabs a');
                const panes2 = document.querySelectorAll('.fruit-tabs .tab-pane');

                tabs2.forEach(function(tab) {
                    tab.classList.remove('active');
                });

                // hide content of all tabs
                panes2.forEach(function(pane) {
                    pane.classList.remove('active');
                });

                // get the tab with href = id
                let selectedTab = document.querySelector('.fruit-tabs .tabs a[href="#' + id + '"]');
                selectedTab.classList.add('active');

                // get the tab with id = id
                let selectedTabContent = document.getElementById(id);
                selectedTabContent.classList.add('active');
            }
        </script>
        <div class="tab-content">
            <?php if (!empty($popular_cars_in_malaysia)) :
                $first_pane = true; // Initialize a flag to track the first pane 
            ?>
                <?php foreach ($popular_cars_in_malaysia as $key => $value) : 
			?>
                    <div id="<?php echo str_replace(' ', '_', $key); ?>-content" class="tab-pane <?php echo $first_pane ? 'active' : ''; ?>">
                        <?php display_popular_bike_posts($value, $brand_id); ?>
                    </div>
                    <?php $first_pane = false; // After the first iteration, set this to false 
                    ?>
                <?php endforeach; ?>
            <?php endif; ?>
        </div>
    </div>
<?php
    return ob_get_clean();
}

add_shortcode('popular_bike_in_new_bikes', 'popular_bike_in_new_bikes');

function popular_bike_tabs_inline_script()
{
    enqueue_find_new_motorcycle_css();
?>
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const tabs = document.querySelectorAll('.fruit-tabs .tabs a');
            const panes = document.querySelectorAll('.fruit-tabs .tab-pane');

            tabs.forEach(function(tab) {
                tab.addEventListener('click', function(event) {
                    event.preventDefault();

                    var totalPages = document.querySelectorAll('.page-btn:not(.prev-btn):not(.next-btn)').length;
                    var currentPage = 1;

                    function showPage(page) {
                        document.querySelectorAll('.car-item').forEach(function(item) {
                            item.style.display = 'none';
                        });

                        document.querySelectorAll('.page-' + page).forEach(function(item) {
                            item.style.display = 'block';
                        });

                        updateMotorPaginationDisplay(page);
                        const prevBtn = document.querySelector('.prev-btn');
                        const nextBtn = document.querySelector('.next-btn');
                        prevBtn.disabled = page === 1;
                        nextBtn.disabled = page === totalPages;

                        // Update the current page variable
                        currentPage = page;
                    }

                    function updateMotorPaginationDisplay(page) {
// 						console.log(page, 'page');
                        document.querySelectorAll('.page-btn').forEach(function(btn) {
                            btn.style.display = 'none';
                        });

                        // Show the first and last page buttons
                        document.querySelector('.page-btn[data-page="1"]').style.display = 'inline-block';
                        document.querySelector('.page-btn[data-page="' + totalPages + '"]').style.display = 'inline-block';
console.log(page);
                        for (var i = page - 2; i <= page + 2; i++) {
                            if (i > 1 && i < totalPages) {
                                document.querySelector('.page-btn[data-page="' + i + '"]').style.display = 'inline-block';
                            }
                        }

                        const existingEllipses = document.querySelectorAll('.ellipsis');
                        existingEllipses.forEach(function(ellipsis) {
                            ellipsis.remove();
                        });

                        if (page > 3) {
                            const firstPageBtn = document.querySelector('.page-btn[data-page="1"]');
                            const ellipsisBefore = document.createElement('span');
                            ellipsisBefore.classList.add('ellipsis');
                            ellipsisBefore.textContent = '...';
                            firstPageBtn.parentNode.insertBefore(ellipsisBefore, firstPageBtn.nextSibling);
                        }

                        if (page < totalPages - 2) {
                            const lastPageBtn = document.querySelector('.page-btn[data-page="' + totalPages + '"]');
                            const ellipsisAfter = document.createElement('span');
                            ellipsisAfter.classList.add('ellipsis');
                            ellipsisAfter.textContent = '...';
                            lastPageBtn.parentNode.insertBefore(ellipsisAfter, lastPageBtn);
                        }

                        // Update the active class on the current page button
                        document.querySelectorAll('.page-btn').forEach(function(btn) {
                            btn.classList.remove('active');
                        });
                        document.querySelector('.page-btn[data-page="' + page + '"]').classList.add('active');
                    }


                    showPage(currentPage);

                    // Remove active class from all tabs and panes
                    tabs.forEach(t => t.classList.remove('active'));
                    panes.forEach(p => p.classList.remove('active'));

                    // Add active class to the clicked tab and corresponding pane
                    tab.classList.add('active');
                    const targetPane = document.querySelector(tab.getAttribute('href'));
                    if (targetPane) {
                        targetPane.classList.add('active');
                    }
                });
            });

            // Set the first tab and pane as active by default
            if (tabs.length > 0 && panes.length > 0) {
                tabs[0].classList.add('active');
                panes[0].classList.add('active');
            }
        });
    </script>
    <?php
}

add_action('wp_footer', 'popular_bike_tabs_inline_script');

function display_popular_bike_posts($bikes, $brand_id = 0)
{
    enqueue_find_new_motorcycle_css();
    $posts_per_page = 9;
    $total_bikes = count($bikes);
    $total_pages = ceil($total_bikes / $posts_per_page);
    $listing_states = [
        'On Sale' => ['label' => 'ฮิต', 'color' => '#F53030'],
        'Not On Sale' => ['label' => 'Not On Sale', 'color' => '#AAAAAA'],
        'Upcoming' => ['label' => 'Upcoming', 'color' => '#32D0C6']
    ];

    $initial_page = 1;
    echo '<script>var currentPage = ' . $initial_page . ';</script>';

    $top_bike_model_ids = get_all_top_bike_model_ids_data(); //fetch_all_top_car_model_ids_from_db();

    if (!empty($bikes)) {
        // Sort cars by listing state, pushing "Not On Sale" to the end
        usort($bikes, function ($car1, $car2) {
            $state1 = $car1['listing_state'];
            $state2 = $car2['listing_state'];

            // "Not On Sale" should be ordered last
            if ($state1 === 'Not On Sale' && $state2 !== 'Not On Sale') {
                return 1;
            } elseif ($state1 !== 'Not On Sale' && $state2 === 'Not On Sale') {
                return -1;
            }
            return 0; // Keep other orders as is
        });

        echo '<div class="car-list">';

        // Display each car, assigning a page class based on its position
        foreach ($bikes as $index => $bike) {
            $page_number = floor($index / $posts_per_page) + 1;

            $bike_id = $bike['id'];
            $bike_title = $bike['post_title'];
            $guid = $bike['thumbnail_url'];
            $price = $bike['price_range'];
            $permalink = $bike['permalink'];
            $post_name = $bike['post_name'];
            $listing_state = $bike['listing_state'];
            $listing_make = $bike['listing_make'];

            $is_hot = false;
            // Check if the car is under 1 lakh and is in the top models
            if (in_array($bike_id, $top_bike_model_ids)) {
                $is_hot = true;
            }

            $state = $is_hot ? ['label' => 'ฮิต', 'color' => '#F53030'] : $listing_states[$listing_state];

            echo '<div class="car-item page-' . $page_number . '" style="display: ' . ($page_number == 1 ? 'block' : 'none') . ';">';
            echo '<a href="' . esc_url($permalink) . '" class="car-link">';
            if ($brand_id != 0) {
                echo '<span class="listing-state" style="background-color: ' . $state['color'] . ';">' . $state['label'] . '</span>';
            }
            echo '<span><img src="' . esc_url($guid) . '" alt="' . esc_attr($bike_title) . '" loading="lazy"></span>';
            echo '<div class="title-and-post">';
            echo '<p class="popular-new-car-find"><span class="car-brand-icon">.</span>' . esc_html($listing_make) . '</p>';
            echo '<div><div class="popular-cars-find">' . esc_html($bike_title) . '</div></div>';
            echo '<span><div class="find-new-cars-price">' . $price . '</div></span>';
            echo '</div>';
            echo '<button class="view-model-button">  Xem xe máy </button>';
            echo '</a>';
            echo '</div>';
        }

        echo '</div>';

    ?>
        <script>
            jQuery(document).ready(function($) {
                var totalPages = $('.page-btn').not('.prev-btn, .next-btn').length;

                function showPage(page) {
                    $('.car-item').hide();
                    $('.page-' + page).show();

                    updateMotorPaginationDisplay(page);

                    $('.prev-btn').prop('disabled', page === 1);
                    $('.next-btn').prop('disabled', page === totalPages);

                    currentPage = page;
                }

                function updateMotorPaginationDisplay(page) {
                    $('.page-btn').hide();

                    // Show the first page, last page, and a few pages around the current page
                    $('.page-btn[data-page="1"]').show();
                    $('.page-btn[data-page="' + totalPages + '"]').show();

                    // Show pages around the current page
                    for (var i = page - 2; i <= page + 2; i++) {
                        if (i > 1 && i < totalPages) {
                            $('.page-btn[data-page="' + i + '"]').show();
                        }
                    }

                    // Show ellipses if there’s a gap between the current page range and the first/last page
                    $('.ellipsis').remove();
                    if (page > 3) {
                        $('<span class="ellipsis">...</span>').insertAfter('.page-btn[data-page="1"]');
                    }
                    if (page < totalPages - 2) {
                        $('<span class="ellipsis">...</span>').insertBefore('.page-btn[data-page="' + totalPages + '"]');
                    }

                    // Update the active class on the current page button
                    $('.page-btn').removeClass('active');
                    $('.page-btn[data-page="' + page + '"]').addClass('active');
                }

                showPage(currentPage);

                // Page number buttons
                $('.page-btn').not('.prev-btn, .next-btn').on('click', function() {
                    var page = $(this).data('page');
                    showPage(page);

                    // Scroll to tabs
                    const fruitTabs = document.querySelector('.fruit-tabs');
                    if (fruitTabs) {
                        fruitTabs.scrollIntoView({
                            behavior: 'smooth',
                            block: 'start'
                        });
                    }
                });

                // Previous button
                $('.prev-btn').on('click', function() {
                    if (currentPage > 1) {
                        showPage(currentPage - 1);
                    }
                });

                // Next button
                $('.next-btn').on('click', function() {
                    if (currentPage < totalPages) {
                        showPage(currentPage + 1);
                    }
                });
            });
        </script>
<?php

        // Pagination controls
        if ($total_pages > 1) {
            echo '<div class="pagination">';
            echo '<button class="page-btn prev-btn" data-page="prev" disabled>Previous</button>';

            for ($i = 1; $i <= $total_pages; $i++) {
                echo '<button class="page-btn" data-page="' . $i . '">' . $i . '</button>';
            }

            echo '<button class="page-btn next-btn" data-page="next">Next</button>';
            echo '</div>';
        }
    } else {
        echo '';
    }
}
?>
