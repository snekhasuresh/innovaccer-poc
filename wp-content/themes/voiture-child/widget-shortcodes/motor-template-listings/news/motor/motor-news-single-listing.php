<?php
function render_motor_news_tab_content($query_args, $is_ajax = false, $page = 1, $posts_per_page = 10)
{
    $query_args['paged'] = $page;
    $query_args['posts_per_page'] = $posts_per_page;

    $news_query = new WP_Query($query_args);
    $output = '';

    $more_posts_available = $news_query->found_posts > ($page * $posts_per_page);
    if ($news_query->have_posts()) {
        while ($news_query->have_posts()) : $news_query->the_post();
            $author_id = get_post_field('post_author', get_the_ID());
            $author_image_url = get_the_author_meta('user_url', $author_id);
            if (!$author_image_url) {
                $author_image_url = get_avatar_url($author_id, ['size' => 32]);
            }

            $author_page_link = get_author_posts_url($author_id);
            $custom_author_link = get_custom_author_post_link($author_id, $author_page_link);

            $output .= '
            <li class="single-listing-news-item">
                <div class="single-listing-news-thumbnail">';

            // Get post thumbnail ID and guid
            $post_thumbnail_id = get_post_thumbnail_id(get_the_ID());
            if ($post_thumbnail_id) {
                $thumbnail_post = get_post($post_thumbnail_id);
                $guid = $thumbnail_post->guid;

                $output .= '<a href="' . get_custom_post_link(get_the_ID(), get_permalink()) . '"><img src="' . esc_url($guid) . '" alt="' . esc_attr(get_the_title()) . '" class="single-listing-news-thumbnail-image"></a>';
            }

            $output .= '
                </div>
                <div class="single-listing-news-content">
                    <h3><a href="' . get_custom_post_link(get_the_ID(), get_permalink()) . '" class="single-listing-news-title">' . get_the_title() . '</a></h3>
                    <div class="single-listing-news-description">' . wp_trim_words(get_the_content(), 20, '...') . '</div>
                    <div class="single-listing-news-meta">
                       <div class="single-listing-news-avatar-con">
					    <a href="' . esc_url($custom_author_link) . '">
                            <span class="news-avatar">
                                <img class="single-listing-news-avatar" src="' . esc_url($author_image_url) . '" />
                                <span class="news-author">' . get_the_author() . ' </span>
								<span class="news-author">
				</span>
                            </span>
                        </a>
					   </div>
                        <span class="single-listing-news-date">' . get_the_date() . '</span>
                    </div>
                </div>
            </li>';
        endwhile;
    } else {
        $output .= '<li>No news content available.</li>';
    }

    wp_reset_postdata();

    if ($is_ajax) {
        echo json_encode(['content' => $output, 'more' => $more_posts_available]);
        die();
    }

    return $output;
}

function motor_news_single_listing_shortcode($atts)
{
    ob_start();

    // Get the post ID for the related car model
    $make = get_query_var('make');
    $listing_name = get_query_var('model');
    $listing_name = $make . '-' . $listing_name;
    $listing_post = get_posts(array(
        'name' => $listing_name,
        'post_type' => 'motorcycle-listing',
        'posts_per_page' => 1
    ));

    $post_id = $listing_post ? $listing_post[0]->ID : '';

    $latest_args = array(
        'post_type' => 'motorcycle-news',
        'posts_per_page' => -1, // Retrieve all related posts (may need adjustment if there are performance issues)
        'meta_query' => array(
            array(
                'key' => 'related_bike_model',
                'value' => $post_id,
                'compare' => 'LIKE'
            ),
            array(
                'key'     => 'second_language',
                'value'   => '',
                'compare' => '='
            )
        ),
    );

    $latest_query = new WP_Query($latest_args);

    $news_categories = array(); // Array to store unique news categories
    $category_names = [];
    if ($latest_query->have_posts()) :
        while ($latest_query->have_posts()) : $latest_query->the_post();
            // Get the news_category metadata for each post
            $news_category = get_post_meta(get_the_ID(), 'motorcycle-news-category', true);

            if ($news_category) {
                $category = get_term($news_category[0], 'motorcycle-news-category');
                if ($category->parent && $category->parent == 0) {
                    $news_categories[] = $news_category[0];
                } else {
                    $get_parent_terms = get_term($category->parent, 'motorcycle-news-category');
                    $news_categories[] = $get_parent_terms->term_id;
                }
            }
        endwhile;
        wp_reset_postdata();
        $all_subcategory_ids = [];
        global $wpdb;
        foreach (array_unique($news_categories) as $term_id) {
            $term = get_term($term_id, 'motorcycle-news-category');

            if (!is_wp_error($term) && $term) {
                $category_names[] = $term;
            } else {
                echo "Error retrieving term ID $term_id: " . ($term->get_error_message() ?? 'Invalid term');
            }
        }
    endif;
?>
    <div id="loader" style="display: none;">
        <i class="fas fa-spinner fa-spin"></i> Loading...
    </div>
    <div class="single-listing-heading-and-select">
        <!-- Tab navigation -->
        <div class="single-listing-news-tabs">
            <ul class="single-listing-tabs-nav">
                <li class="single-listing-tab-link active" data-tab="latest">ล่าสุด</li>
                <?php if (!empty($category_names)) :
                    $all_subcategory_ids = [];
                    foreach ($category_names as $category) :
                        $slug = strtolower(str_replace(' ', '-', $category->slug));

                        $query = $wpdb->prepare("
                        SELECT term_id
                        FROM {$wpdb->term_taxonomy} AS t
                        WHERE t.parent = %d
                        AND t.taxonomy = %s
                        ", $category->term_id, 'motorcycle-news-category');

                        $subcategories = $wpdb->get_results($query);

                        $subcategory_ids = [];
                        foreach ($subcategories as $subcategory) {
                            $subcategory_ids[] = $subcategory->term_id;
                        }

                        $all_subcategory_ids = $subcategory_ids;
                ?>

                        <li class="single-listing-tab-link" data-tab="<?php echo esc_attr($slug); ?>" data-term-id="<?php echo esc_attr($category->term_id); ?>" data-subcategories='<?php echo esc_attr(json_encode($all_subcategory_ids)); ?>'>
                            <?php echo esc_html($category->name); ?>
                        </li>
                    <?php endforeach; ?>
                <?php endif; ?>
            </ul>
        </div>
    </div>

    <div id="latest" class="single-listing-tab-content">
        <ul class="news-list" data-tab-id="latest">
            <?php
            // Query args for latest news
            $latest_args = array(
                'post_type' => 'motorcycle-news',
                'posts_per_page' => 10,
                'orderby' => 'date',
                'order' => 'DESC',
                'meta_query' => array(
                    array(
                        'key' => 'related_bike_model',
                        'value' => $post_id,
                        'compare' => 'LIKE'
                    ),
                    array(
                        'key'     => 'second_language',
                        'value'   => '',
                        'compare' => '='
                    )
                ),
            );

            echo render_motor_news_tab_content($latest_args);
            ?>
        </ul>
        <?php
        $count_query = new WP_Query(array_merge($latest_args, ['posts_per_page' => -1]));
        $total_posts = $count_query->found_posts;

        if ($total_posts > 10) {

            echo ' <div class="view-more-container">
                          <a  class="view-more" data-page="1" data-tab-id="latest">
                               ดูเพิ่มเติม
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                             </svg>

                             </a>
                         </div>';
        }

        wp_reset_postdata();
        ?>
    </div>

    <?php
    $all_subcategory_ids = [];
    foreach ($category_names as $category) :
        $slug = strtolower(str_replace(' ', '-', $category->slug));

        $query = $wpdb->prepare("
                        SELECT term_id
                        FROM {$wpdb->term_taxonomy} AS t
                        WHERE t.parent = %d
                        AND t.taxonomy = %s
                        ", $category->term_id, 'motorcycle-news-category');

        $subcategories = $wpdb->get_results($query);

        $subcategory_ids = [];
        foreach ($subcategories as $subcategory) {
            $subcategory_ids[] = $subcategory->term_id;
        }

        $all_subcategory_ids = $subcategory_ids;
    ?>

        <div id="<?php echo esc_attr($slug); ?>" class="single-listing-tab-content" style="display:none;">
            <ul class="news-list" data-tab-id="<?php echo esc_attr($slug); ?>">
                <?php
                // Your news items go here
                wp_reset_postdata();
                ?>
            </ul>
            <!-- Move the view-more-container outside the news-list ul -->
            <div class="view-more-container" style="display: none;">
                <a class="view-more" data-page="1" data-tab-id="<?php echo esc_attr($slug); ?>" data-term-id="<?php echo esc_attr($category->term_id); ?>" data-subcategories="<?php echo esc_attr(json_encode($all_subcategory_ids)); ?>">
                    ดูเพิ่มเติม
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                    </svg>
                </a>
            </div>
        </div>
    <?php endforeach; ?>

    <script>
        document.addEventListener('DOMContentLoaded', function() {
            // Show the latest news tab by default
            const defaultTab = 'latest';
            document.querySelectorAll('.single-listing-tab-content').forEach(content => {
                content.style.display = content.id === defaultTab ? 'block' : 'none';
            });

            document.querySelectorAll('.single-listing-tab-link').forEach(tab => {
                tab.addEventListener('click', function() {
                    document.querySelectorAll('.single-listing-tab-link').forEach(el => el.classList.remove('active'));
                    tab.classList.add('active');

                    const targetTab = tab.getAttribute('data-tab');
                    const termId = tab.getAttribute('data-term-id');
                    let subcategories = [];

                    // Fetch the subcategories for the currently selected tab
                    const subcategoriesString = tab.getAttribute('data-subcategories');

                    if (subcategoriesString) {
                        subcategories = JSON.parse(subcategoriesString); // Parse the JSON string
                    }

                    document.querySelectorAll('.single-listing-tab-content').forEach(content => {
                        content.style.display = content.id === targetTab ? 'block' : 'none';
                    });

                    if (termId && targetTab !== 'latest' && subcategories) {
                        const newsList = document.querySelector(`#${targetTab} .news-list`);

                        if (newsList) {
                            newsList.innerHTML = ''; // Clear previous content
                            loadNews(termId, targetTab, subcategories, 1);
                        }
                    }
                });
            });

            function loadNews(termId, targetTab, subcategories, page) {
                const data = new FormData();
                data.append('action', 'load_motor_category_news');
                data.append('term_id', termId);
                data.append('tab_id', targetTab);
                data.append('post_id', '<?php echo $post_id; ?>');
                data.append('subcategories', subcategories);
                data.append('page', page);
                const loader = document.getElementById('loader');
                loader.style.display = 'block';

                fetch('<?php echo admin_url('admin-ajax.php'); ?>', {
                        method: 'POST',
                        body: data
                    })
                    .then(response => response.text())
                    .then(data => {
                        loader.style.display = 'none';
                        let parsedData = JSON.parse(data);
                        const newsList = document.querySelector(`#${targetTab} .news-list`);
                        newsList.innerHTML = parsedData.content; // Populate the news list

                        const viewMoreContainer = document.querySelector(`#${targetTab} .view-more-container`);
                        let viewMoreButton = viewMoreContainer.querySelector('.view-more');
                        viewMoreContainer.style.display = 'block';
                        viewMoreButton.setAttribute('data-page', page);

                        if (!parsedData.more) {
                            viewMoreContainer.style.display = 'none';
                        }
                    });
            }

            document.querySelectorAll('.view-more').forEach(button => {
                button.addEventListener('click', function() {
                    const page = parseInt(button.getAttribute('data-page')) + 1;
                    const tabId = button.getAttribute('data-tab-id');
                    const termId = button.getAttribute('data-term-id');
                    const subcategoriesString = document.querySelector(`li[data-tab="${tabId}"]`).getAttribute('data-subcategories');
                    let subcategories = [];

                    if (subcategoriesString) {
                        subcategories = JSON.parse(subcategoriesString); // Parse the JSON string
                    }

                    const data = new FormData();
                    data.append('action', 'motor_load_more_individual_news');
                    data.append('page', page);
                    data.append('tab_id', tabId);
                    data.append('term_id', termId);
                    data.append('post_id', '<?php echo $post_id; ?>');
                    data.append('subcategories', subcategories);
                    const loader = document.getElementById('loader');
                    loader.style.display = 'block';
                    button.style.display = 'none';
                    fetch('<?php echo admin_url('admin-ajax.php'); ?>', {
                            method: 'POST',
                            body: data
                        })
                        .then(response => response.text())
                        .then(data => {
                            loader.style.display = 'none';
                            let parsedData = JSON.parse(data);
                            const newsList = document.querySelector(`#${tabId} .news-list`);
                            newsList.insertAdjacentHTML('beforeend', parsedData.content);
                            button.style.display = 'block';
                            button.style.display = parsedData.more ? 'block' : 'none';
                            button.setAttribute('data-page', page);
                        });
                });
            });
        });
    </script>
    <style>
        .view-more {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 5px;
            margin: 24px auto 0;
            padding: 8px 16px;
            border: none;
            background: none;
            color: #576B95;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
        }

        .view-more svg {
            width: 18px;
            height: 20px;
        }

        .view-more-container {
            text-align: center;
            margin-top: 20px;
        }

        /* Tabs */
        .single-listing-tabs-nav {
            list-style-type: none;
            padding-top: 14px;
            margin: 0;
            display: flex;
            border-bottom: 2px solid #ddd;
            width: 129%;
            position: absolute;
            top: -15px;
            z-index: 1;
        }

        .single-listing-news-avatar-con {
            display: flex;
            align-items: center;
        }

        .single-listing-tabs-nav li {
            margin-right: 20px;
            padding: 5px 0%;
            cursor: pointer;
            font-size: 15px;
            font-weight: bold;
            font-family: 'Roboto';
            color: #8c8c8c;
            border-bottom: 3px solid transparent;
        }

        .single-listing-tabs-nav li.active {
            color: #262626;
            border-bottom: 3px solid #ffb400;
            font-family: 'Roboto';
            margin-bottom: -2px;
        }

        .single-listing-tab-content {
            margin-top: 20px;
        }

        /* News items */
        .single-listing-news-item {
            display: flex;
            align-items: center;
            padding: 40px 0;
        }

        .single-listing-news-thumbnail {
            flex-shrink: 0;
            margin-right: 20px;
            display: flex;
        }

        .single-listing-news-thumbnail img {
            width: 400px;
            height: 230px;
            border-radius: 8px;
        }

        .single-listing-news-content {
            flex-grow: 1;
            margin-top: -30px;
        }

        .single-listing-news-title {
            font-size: 26.91px;
            font-weight: bold;
            color: #262626;
            text-decoration: none;
            overflow: hidden;
            text-overflow: ellipsis;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            line-height: 1.3;
            height: calc(1.4em* 2);
            font-family: 'Roboto Condensed';
            margin-bottom: 10px;
        }

        .single-listing-news-title:hover {
            color: #ffb400;
        }

        .single-listing-news-description {
            font-size: 14px;
            line-height: 1.6;
            margin-top: -27px;
            font-size: 14px;
            font-family: "Roboto";
            color: #8c8c8c;
            line-height: 22px;
            display: -webkit-box;
            overflow: hidden;
            text-overflow: ellipsis;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            word-break: break-word;
        }

        .single-listing-news-meta {
            display: flex;
            align-items: center;
            margin-top: 70px;
        }

        .single-listing-news-author {
            margin-left: 8px;
            font-size: 14px;
            font-family: "Roboto";
            color: #8c8c8c;
            line-height: 20px;
        }

        .single-listing-news-avatar {
            border-radius: 50% !important;
            height: 30px !important;
        }

        .single-listing-news-date {
            margin-left: auto;
            font-size: 14px;
            font-family: "Roboto";
            color: #8c8c8c;
            line-height: 20px;
        }

        .view-more-button {
            background-color: #32D0C6;
            color: white;
            border: none;
            padding: 10px 20px;
            cursor: pointer;
            display: block;
            margin: 20px auto;
            border-radius: 5px;
            font-size: 14px;
        }

        .view-more-button:hover {
            background-color: #2ACDAE;
        }

        @media (min-width: 768px) and (max-width: 1024px) {
            .single-listing-news-item {
                display: flex;
                align-items: center;
                padding: 40px 0;
                flex-direction: column;
            }

            .single-listing-news-content {
                flex-grow: 1;
                margin-top: 0px;
            }

            .single-listing-news-thumbnail {
                flex-shrink: 0;
                margin-right: 20px;
                display: flex;
                width: 100%;
            }

            .single-listing-news-thumbnail img {
                width: 100%;
                height: 100%;
                border-radius: 8px;
            }
        }

        @media (max-width: 768px) {
            .single-listing-news-item {
                display: flex;
                align-items: center;
                padding: 40px 0;
                flex-direction: column;
            }

            .single-listing-news-content {
                flex-grow: 1;
                margin-top: 0px;
            }

            .single-listing-news-thumbnail {
                flex-shrink: 0;
                margin-right: 20px;
                display: flex;
                width: 100%;
            }

            .single-listing-news-thumbnail img {
                width: 100%;
                height: 100%;
                border-radius: 8px;
            }

            .single-listing-news-date {
                margin-left: 0;
                margin-top: 10px;
            }
        }

        #loader {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 24px;
            color: #333;
            z-index: 1000;
        }
    </style>


<?php
    return ob_get_clean();
}
add_shortcode('motor_news_single_listing', 'motor_news_single_listing_shortcode');

add_action('wp_ajax_motor_load_more_individual_news', 'motor_load_more_individual_news');
add_action('wp_ajax_nopriv_motor_load_more_individual_news', 'motor_load_more_individual_news');

add_action('wp_ajax_load_motor_category_news', 'load_motor_category_news');
add_action('wp_ajax_nopriv_load_motor_category_news', 'load_motor_category_news');

function motor_load_more_individual_news()
{
    $paged = isset($_POST['page']) ? intval($_POST['page']) : 1;
    $post_id = intval($_POST['post_id']);
    $posts_per_page = 10;
    $subcategories = isset($_POST['subcategories']) ? explode(',', $_POST['subcategories']) : [];
    // $subcategoryArray = array_map('intval', $subcategories);
    $subcategoryArray = array_filter(array_map('intval', $subcategories), function ($value) {
        return $value !== 0;
    });
    $meta_query = array(
        array(
            'key' => 'related_bike_model',
            'value' => $post_id,
            'compare' => 'LIKE'
        ),
        array(
            'key'     => 'second_language',
            'value'   => '',
            'compare' => '='
        )
    );

    // Add term_id condition only if it's not 0
    if (!empty($subcategoryArray)) {
        // Create a meta query for each subcategory
        $subcategory_meta_query = array(
            'relation' => 'OR' // This ensures that it matches any of the subcategories
        );

        foreach ($subcategoryArray as $subcategory_id) {
            $subcategory_meta_query[] = array(
                'key' => 'motorcycle-news-category',
                'value' => '"' . $subcategory_id . '"', // Searching for serialized integer
                'compare' => 'LIKE'
            );
        }
        // Add the subcategory conditions to the main meta query
        $meta_query[] = $subcategory_meta_query;
    }

    $args = array(
        'post_type' => 'motorcycle-news',
        'posts_per_page' => $posts_per_page,
        'paged' => $paged,
        'meta_query' => $meta_query,
    );

    $news_content = render_motor_news_tab_content($args, true, $paged);
    echo $news_content;
    die();
}

function load_motor_category_news()
{
    $paged = isset($_POST['page']) ? intval($_POST['page']) : 1;
    $post_id = intval($_POST['post_id']);
    $subcategories = isset($_POST['subcategories']) ? explode(',', $_POST['subcategories']) : [];
    $subcategoryArray = $subcategories ? array_map('intval', $subcategories) :  [];


    $posts_per_page = 10;

    $meta_query = array(
        array(
            'key' => 'related_bike_model',
            'value' => $post_id,
            'compare' => 'LIKE'
        ),
        array(
            'key'     => 'second_language',
            'value'   => '',
            'compare' => '='
        )
    );
    // Add term_id condition only if it's not 0
    if (!empty($subcategoryArray)) {
        // Create a meta query for each subcategory
        $subcategory_meta_query = array(
            'relation' => 'OR' // This ensures that it matches any of the subcategories
        );

        foreach ($subcategoryArray as $subcategory_id) {
            $subcategory_meta_query[] = array(
                'key' => 'motorcycle-news-category',
                'value' => '"' . $subcategory_id . '"', // Searching for serialized integer
                'compare' => 'LIKE'
            );
        }

        // Add the subcategory conditions to the main meta query
        $meta_query[] = $subcategory_meta_query;
    }

    $args = array(
        'post_type' => 'motorcycle-news',
        'posts_per_page' => 10,
        'paged' => $paged,
        'meta_query' => $meta_query,
    );


    echo render_motor_news_tab_content($args, true, $paged, $posts_per_page);
}

?>