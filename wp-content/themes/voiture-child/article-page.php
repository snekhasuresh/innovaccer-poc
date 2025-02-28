<?php
function get_current_post_data_shortcode($atts)
{

    $atts = shortcode_atts(array(
        'is_amp' => 0,
    ), $atts, 'current_post_data');


    $news_id = get_last_numeric_id_from_url();
    if ($news_id != NULL) {
        global $wpdb;
        $result = $wpdb->get_var(
            $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $news_id)
        );

        $result = $result ? $result : $news_id;
        $post = $result ? get_post($result) : false;
        if (!$post) {
            return;
        }
    } else {
        // Get the current URL path
        $current_url = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');
        // Extract the slug (last part of the URL)
        $slug = basename($current_url); // This gets the last segment of the URL
        // Get the post ID based on the slug and post type
        $post = get_page_by_path($slug, OBJECT, 'news'); // Replace 'news' with your custom post type
    }

    //     if ($atts['is_amp'] == 1) {
    //         add_ga_tag_for_amp($post);
    //         return get_amp_content($post);
    //     }

    if ($post) {
        $related_model_ids = get_post_meta($post->ID, 'related_car_model', true);
        $make = '';
        $model = '';
        $model_data = '';

        if (!empty($related_model_ids)) {
            if (!is_array($related_model_ids)) {
                $related_model_ids = explode(',', $related_model_ids);
            }

            foreach ($related_model_ids as $model_id) {
                $model_id = intval(trim($model_id));
                $model_post = get_post($model_id);

                if ($model_post && !is_wp_error($model_post)) {
                    $full_title = $model_post->post_title;

                    // Split the title into make and model by the first space
                    $title_parts = explode(' ', $full_title, 2);
                    $make = isset($title_parts[0]) ? $title_parts[0] : '';
                    $model = isset($title_parts[1]) ? $title_parts[1] : '';

                    $model_data .= '[individual_listing_tabs make="' . esc_attr(strtolower(strtolower($make))) . '" model="' . esc_attr(strtolower(strtolower($model))) . '" selected_tab="News"]';
                }
            }
        }
        // Get author details and publish date with time
        $author_id = $post->post_author;
        $author_name = get_the_author_meta('display_name', $author_id);
        $publish_date = get_post_meta($post->ID, 'publish_time', true);
        $author_image_url = ''; //get_image_url($post, $image_type = 'author'); // get_the_author_meta('user_url', $author_id);
        // Start output buffering
        ob_start();

        // Display post title, author avatar, author name, publish date, and content with inline CSS
?>
        <!--         <section>
			 <div class="related-tabs">
            <?php echo do_shortcode($model_data); ?>
        </div>
		</section> -->

        <div style="max-width: 100%; margin: 0 auto; font-family: Arial, sans-serif;">

            <h1 class="pre-head"><?php echo esc_html($post->post_title); ?></h1>

            <div style="display: flex; align-items: center; gap: 8px; color: #555; margin-top: -3px; font-size: 16px;">
                <div style="flex-shrink: 0; border-radius: 50%; overflow: hidden; width: 30px;">
                    <img class="author-profile" src="<?php echo get_image_url($author_id, 'author'); ?>" alt="<?php echo esc_attr($author_name); ?>" style="border-radius: 50%;">
                </div>
                <div style="display: flex; align-items: center; gap: 5px;">
                    <span class="article-" style="font-weight: 700; color: #576a8f;text-transform: capitalize;"><?php echo esc_html($author_name); ?></span>
                    <span style="color: #888;">·</span>
                    <span class="article-publish"><?php echo esc_html($publish_date); ?></span>
                </div>
            </div>

            <div class="individual-news-des">
                <?php
                $post_content = apply_filters('the_content', $post->post_content);
                // append ad content after 4th p tag
                $content_parts = explode('</p>', $post_content);
                $content_parts[4] .= do_shortcode('[dynamic_ad_unit ad_id="MY_Article_Fourthp_Under_PC"]');
                echo implode('</p>', $content_parts);

                ?>
            </div>
        </div>
        <div>
            <?php
            // echo get_social_container();
            echo get_author_details($author_id);
            ?>
        </div>

        <style>
            .pre-head {
                font-size: 48px;
                font-family: "Roboto Condensed";
                color: #262626;
                line-height: 58px;
                letter-spacing: -0.02em;
                font-weight: 700;
                padding: 8px 0;
                margin-bottom: 16px;
                display: -webkit-box;
                -webkit-box-orient: vertical;
                overflow: hidden;
                text-overflow: ellipsis;
            }

            .article-publish {
                font-size: 16px;
                font-family: "Roboto";
                color: #8c8c8c;
                line-height: 22px;
                margin-left: 10px;
            }

            .individual-news-des {
                word-break: break-word;
                font-size: 16px;
                line-height: 1.5;
                margin-bottom: 24px !important;
                font-family: 'Roboto';
                margin-top: 38px;
                color: #262626
            }

            .individual-news-des p {
                margin-bottom: 20px;
            }

            .related-tabs {
                width: 100%;
            }

            .author-profile {
                heigth: 30px !important;
            }

            @media screen and (max-width: 768px) {
                .header-tabs {
                    background-color: #0A2357;
                    display: flex;
                    text-align: center;
                    align-items: center;
                    padding: 0;
                    width: 100%;
                    overflow-x: scroll;
                    margin: 10px 0;
                    height: 64px;
                    /*                     margin-left: -126px; */
                    scrollbar-width: none;
                }

                .pre-head {
                    font-size: 30px;
                    color: #262626;
                    font-family: "Roboto Condensed";
                    font-weight: 700;
                    line-height: 40px;
                }
            }
        </style>
    <?php

        return ob_get_clean(); // Return the buffered content
    } else {
        return 'Invalid slug or post not found.';
    }
}

// Register the shortcode
add_shortcode('current_post_data', 'get_current_post_data_shortcode');

function get_amp_content($post)
{
    ob_start();
    global $ad_unit;
    $ad_unit_4p_tag = "<!-- /22557728108/my_article_fourthp_under_wap -->
        <div id='div-gpt-ad-1735896007301-0' style='min-width: 300px; min-height: 250px;'>
        <script>
            googletag.cmd.push(function() { googletag.display('div-gpt-ad-1735896007301-0'); });
        </script>
        </div>";

    $ad_unit_below_author = "<!-- /22557728108/my_article_fourthp_under_wap -->
        <div id='div-gpt-ad-1735896007301-0' style='min-width: 300px; min-height: 250px;'>
        <script>
            googletag.cmd.push(function() { googletag.display('div-gpt-ad-1735896007301-0'); });
        </script>
        </div>";

    $title = $post->post_title;
    $content = $post->post_content;
    $author_id = $post->post_author;

    $author_name = get_the_author_meta('display_name', $author_id);
    $publish_date = get_the_date('M j, Y h:i A', $post);
    $author_description = get_the_author_meta('description', $author_id);
    $author_title = get_the_author_meta('title', $author_id);
    $author_image_url = get_the_author_meta('user_url', $author_id);

    // add ad-unit after 4th p tag
    $content = apply_filters('the_content', $content);
    $content_parts = explode('</p>', $content);
    $content_parts[4] .= $ad_unit_4p_tag;
    $content = implode('</p>', $content_parts);

    // add ad-unit at the end of the content
    $content .= $ad_unit;

    $amp_title = convertToAmpHtml($title);
    $amp_content = convertToAmpHtml($content);
    ?>
    <html amp>

    <head>
        <meta charset="utf-8">
        <title><?php $post->title; ?></title>
        <link rel="canonical" href="<?php the_permalink(); ?>">
        <meta name="viewport" content="width=device-width,minimum-scale=1,initial-scale=1">
        <style amp-custom>
            body {
                font-family: Arial, sans-serif;
                padding: 10px;
            }

            h1 {
                font-size: 24px;
            }
        </style>
        <script async src="https://cdn.ampproject.org/v0.js"></script>
    </head>

    <body>
        <h1><?php echo $amp_title; ?></h1>
        <div>
            <div style="display: flex; align-items: center; gap: 10px;">
                <img src="<?php echo esc_url($author_image_url); ?>" alt="<?php echo esc_attr($author_name); ?>" width="24" height="24" style="border-radius: 50%;">
                <h3>
                    <span style="font-size:18px;color:#576b95;font-family:'Roboto';font-weight:700; text-transform: capitalize;"><?php echo esc_html($author_name); ?></span>
                    <span style="font-size:16px;color:#8c8c8c;font-family:'Roboto';"><?php echo esc_html($publish_date); ?></span>
                </h3>
            </div>
        </div>

        <div><?php echo $amp_content ?></div>
        <?php echo get_social_container(); ?>
        <?php echo get_author_details($author_id, true); ?>
        <?php echo $ad_unit_below_author; ?>
        <?php echo do_shortcode('[latest_news_shortcode]'); ?>
        <?php echo $ad_unit; ?>
        <?php echo do_shortcode('[recommended_cars]'); ?>
        <?php echo $ad_unit; ?>
        <?php echo do_shortcode('[popular_car_brands]'); ?>
        <?php echo $ad_unit; ?>
    </body>

<?php
    return ob_get_clean();
}

function get_author_details($author_id, $is_amp = false)
{
    if ($is_amp) {
        return get_amp_author_details($author_id);
    }

    return get_non_amp_author_details($author_id);
}

function get_amp_author_details($author_id)
{
    $author_name = get_the_author_meta('display_name', $author_id);
    $author_image_url = get_the_author_meta('user_url', $author_id);
    $author_title = get_the_author_meta('title', $author_id);
    $author_description = get_the_author_meta('description', $author_id);

    ob_start();
?>
    <div>
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="<?php echo esc_url($author_image_url); ?>" alt="<?php echo esc_attr($author_name); ?>" width="64" height="64" style="border-radius: 50%;">
            <h3>
                <span class="first-letter-capital"><?php echo esc_html($author_name); ?></span>
                <span class="first-letter-capital" style="font-size:small"><?php echo esc_html($author_title); ?></span>
            </h3>

        </div>
        <p><?php echo esc_html($author_description); ?></p>
    </div>

<?php
    return ob_get_clean();
}

function get_non_amp_author_details($author_id)
{
    $author_name = get_the_author_meta('display_name', $author_id);
    $author_image_url = get_image_url($author_id, $image_type = 'author'); // get_the_author_meta('user_url', $author_id);
    $author_title = get_the_author_meta('title', $author_id);
    $author_description = get_the_author_meta('description', $author_id);

    ob_start();
?>
    <div class="author-box">
        <div class="author-avatar">
            <img src="<?php echo get_image_url($author_id, 'author'); ?>" alt="<?php echo esc_attr($author_name); ?>" width="64" height="64" style="border-radius: 50%;">
        </div>
        <div class="author-info">
            <h3 class="author-name">
                <span class="author-name first-letter-capital"><?php echo esc_html($author_name); ?></span>
                <span class="author-title first-letter-capital"><?php echo esc_html($author_title); ?></span>
            </h3>
            <p class="author-description"><?php echo esc_html($author_description); ?></p>
        </div>
    </div>

    <style>
        .author-box {
            display: flex;
            align-items: center;
            border: 1px solid #ddd;
            padding: 20px;
            border-radius: 8px;
            background-color: #fff;
        }

        .author-avatar {
            width: 20%;
            padding: 10px;
        }

        .author-avatar img {
            border-radius: 50%;
            width: 120px;
            height: 90px;
        }

        .individual-news-author-name {
            font-size: 18px;
            color: #262626;
            font-family: 'Roboto';
            font-weight: 700;
            text-transform: capitalize;
        }

        .author-info {
            flex-grow: 1;
            padding-left: 20px;
            border-top: none !important;
        }

        .author-name {
            font-size: 20px;
            margin: 0;
            font-weight: 600;
        }

        .author-title {
            font-size: 14px;
            color: #777;
            margin-left: 10px;
        }

        .author-description {
            font-size: 14px;
            font-weight: 400;
            color: #595959;
            line-height: 1.5;
            white-space: pre-line;
        }

        .author-info .author-title {
            height: 24px;
            font-size: 14px;
            font-weight: 400;
            color: #8c8c8c;
        }

        .author-box {
            border-radius: 0px !important;
            padding: 0px !important;
        }

        .share-icon {
            position: fixed;
            left: 200px;
            top: 252px;
        }

        @media screen and (max-width: 768px) {
            .author-box {
                display: flex;
                align-items: center;
                flex-direction: column;
                border: 1px solid #ddd;
                padding: 20px;
                border-radius: 8px;
                background-color: #fff;
            }

            .author-avatar {
                width: 28%;
                padding: 10px;
            }
        }
    </style>
<?php
    return ob_get_clean();
}

function get_social_container()
{

    wp_enqueue_style('font-awesome', 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css');
?>

    <!-- HTML -->
    <div class="fsm-container">
        <h2 class="fsm-heading">Follow our socials:</h2>
        <div class="fsm-buttons-wrapper">
            <a href="https://www.youtube.com/channel/UCiS-ROA1ZyuprmjR00innWg" class="fsm-btn fsm-btn-youtube">
                <i class="fab fa-youtube"></i> YouTube
            </a>
            <a href="https://m.facebook.com/wapcar.my/" class="fsm-btn fsm-btn-facebook">
                <i class="fab fa-facebook-f"></i> FaceBook
            </a>
        </div>
    </div>

    <style>
        .fsm-container {
            margin: 20px 0;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen-Sans, Ubuntu, Cantarell, "Helvetica Neue", sans-serif;
        }

        .fsm-heading {
            color: #333;
            font-size: 18px;
            margin-bottom: 10px;
            font-weight: 600;
        }

        .fsm-buttons-wrapper {
            display: flex;
            gap: 10px;
        }

        .fsm-btn {
            display: inline-flex;
            align-items: center;
            padding: 5px 12px;
            border-radius: 2px;
            color: white;
            text-decoration: none;
            font-size: 14px;
            transition: opacity 0.2s;
        }

        .fsm-btn:hover {
            opacity: 0.9;
        }

        .fsm-btn i {
            margin-right: 6px;
        }

        .fsm-btn-youtube {
            background-color: #FF0000;
        }

        .fsm-btn-facebook {
            background-color: #1877F2;
        }
    </style>

<?php
    return ob_get_clean();
}

function convertToAmpHtml($htmlContent)
{
    // Convert standard images to AMP images
    $htmlContent = preg_replace(
        '/<img([^>]+)src="([^"]+)"([^>]*)>/i',
        '<amp-img$1src="$2"$3 width="300" height="200" layout="responsive"></amp-img>',
        $htmlContent
    );

    // Convert standard videos (YouTube) to AMP videos
    $htmlContent = preg_replace(
        '/<iframe[^>]*src="https:\/\/www\.youtube\.com\/embed\/([^"]+)"[^>]*><\/iframe>/i',
        '<amp-youtube data-videoid="$1" width="480" height="270" layout="responsive"></amp-youtube>',
        $htmlContent
    );

    // Remove unsupported scripts
    $htmlContent = preg_replace(
        '/<script\b[^>]*>(.*?)<\/script>/is',
        '',
        $htmlContent
    );

    // Replace standard styles with AMP-compliant styles
    $htmlContent = preg_replace(
        '/<style\b[^>]*>(.*?)<\/style>/is',
        '<style amp-custom>$1</style>',
        $htmlContent
    );

    // Convert standard audio to AMP audio
    $htmlContent = preg_replace(
        '/<audio([^>]*)>(.*?)<\/audio>/i',
        '<amp-audio$1>$2</amp-audio>',
        $htmlContent
    );

    // Replace standard forms with AMP forms (basic example)
    $htmlContent = preg_replace(
        '/<form([^>]*)>/i',
        '<form$1 method="post" action-xhr="/submit-form">',
        $htmlContent
    );

    // Ensure links have valid rel attributes for AMP
    $htmlContent = preg_replace(
        '/<a([^>]+)href="([^"]+)"([^>]*)>/i',
        '<a$1href="$2"$3 rel="noopener">',
        $htmlContent
    );

    // Add AMP boilerplate if not present
    if (!strpos($htmlContent, '<html amp')) {
        $htmlContent = str_replace('<html', '<html amp', $htmlContent);
        $htmlContent = "<!doctype html>\n<html amp>\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width,minimum-scale=1,initial-scale=1\">\n<script async src=\"https://cdn.ampproject.org/v0.js\"></script>\n</head>\n<body>\n" . $htmlContent . "\n</body>\n</html>";
    }

    return $htmlContent;
}

// Function to add the AMP Analytics script to <head>
function add_amp_analytics_script()
{
    echo '<script async custom-element="amp-analytics" src="https://cdn.ampproject.org/v0/amp-analytics-0.1.js"></script>';
}

// Function to add the AMP Analytics JSON-LD to <body>
function add_amp_analytics_json($post)
{
    if (is_null($post)) {
        return; // Ensure $post is valid
    }

    $lang_map = [
        '' => 'en',
        'my-my' => 'my',
        'my-zh' => 'zh'
    ];
    $author_id = $post->post_author;
    $current_url = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');

    $news_author = get_the_author_meta('display_name', $author_id);
    $news_category = '';
    $news_type = 'article';
    $news_id = $post->ID;
    $news_title = get_the_title($post->ID);
    $news_language = $lang_map[get_post_meta($post->ID, 'second_language', true)];
    $make = '';
    $model = '';
    $page_url = $current_url;
    $page_path = $current_url;

    $related_car_models = get_post_meta($post->ID, 'related_car_model', true);
    if ($related_car_models) {
        $related_car_id = $related_car_models[0];
        if ($related_car_id) {
            $make_term_id = get_post_meta($related_car_id, '_listing_make', true);
            $make = get_term($make_term_id)->name;
            $model = get_the_title($related_car_id);
        }
    }

    $news_categories = get_post_meta($post->ID, 'news_category', true);
    if ($news_categories) {
        $news_category_id = $news_categories[0];
        if ($news_category_id) {
            $news_category = get_term($news_category_id)->name;
        }
    }
?>
    <amp-analytics type="gtag" data-credentials="include">
        <script type="application/json">
            {
                "vars": {
                    "gtag_id": "G-DQF4SJ9V3Y",
                    "config": {
                        "G-DQF4SJ9V3Y": {
                            "groups": "default"
                        }
                    }
                },
                "triggers": {
                    "newsPageViewed": {
                        "on": "visible",
                        "request": "event",
                        "event_name": "NewsPage_Viewed",
                        "vars": {
                            "news_author": "<?php echo $news_author; ?>",
                            "news_category": "<?php echo $news_category; ?>",
                            "news_type": "<?php echo $news_type; ?>",
                            "news_id": "<?php echo $news_id; ?>",
                            "news_title": "<?php echo $news_title; ?>",
                            "news_language": "<?php echo $news_language; ?>",
                            "make": "<?php echo $make; ?>",
                            "model": "<?php echo $model; ?>",
                            "page_url": "<?php echo $page_url; ?>",
                            "page_path": "<?php echo $page_path; ?>",
                        }
                    }
                }
            }
        </script>
    </amp-analytics>
    <?php
}

// Function to add GA tags for AMP, combining both script and JSON
function add_ga_tag_for_amp($post)
{
    add_amp_analytics_script();
    add_amp_analytics_json($post);
}

// layout for news preview
function custom_news_preview_redirect()
{
    if (is_preview() && get_post_type() == 'news') {
        global $post;

        if (!$post) {
            wp_die('No preview available.');
        }

        get_header(); // Load the site's header
    ?>
        <style>
            .news-preview-wrapper {
                display: flex;
                max-width: 1200px;
                margin: 40px auto;
                padding: 20px;
                background: #fff;
                border-radius: 8px;
                /*                 box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1); */
            }

            .news-content-container {
                width: 70%;
                padding-right: 20px;
            }

            .news-sidebar {
                width: 30%;
                padding-left: 20px;
                /*                 border-left: 1px solid #ddd; */
            }

            .news-preview-container h1 {
                font-size: 28px;
                color: #071A40;
                margin-bottom: 15px;
            }

            .news-meta {
                font-size: 14px;
                color: #777;
                margin-bottom: 20px;
            }

            .news-content {
                font-size: 16px;
                color: #333;
                line-height: 1.6;
            }

            .news-image img {
                width: 100%;
                height: auto;
                border-radius: 5px;
                margin-bottom: 20px;
            }

            .news-sidebar h3 {
                font-size: 20px;
                color: #071A40;
                margin-bottom: 10px;
            }

            .news-sidebar .widget {
                margin-bottom: 20px;
                padding: 15px;
                /*                 background: #f9f9f9; */
                border-radius: 5px;
            }

            .pre-head {
                font-size: 48px;
                font-family: "Roboto Condensed";
                color: #262626;
                line-height: 58px;
                letter-spacing: -0.02em;
                font-weight: 700;
                padding: 8px 0;
                margin-bottom: 16px;
                display: -webkit-box;
                -webkit-box-orient: vertical;
                overflow: hidden;
                text-overflow: ellipsis;
            }
        </style>

        <div class="news-preview-wrapper">
            <!-- Left Side - News Content -->
            <div class="news-content-container">
                <h1 class="pre-head"><?php echo esc_html(get_the_title($post->ID)); ?></h1>
                <!--                 <div class="news-meta">
                    Published on: <?php echo get_the_date('F j, Y', $post->ID); ?>
                </div> -->
                <?php if (has_post_thumbnail($post->ID)) : ?>
                    <div class="news-image">
                        <?php echo get_the_post_thumbnail($post->ID, 'large'); ?>
                    </div>
                <?php endif; ?>
                <div class="news-content">
                    <?php echo apply_filters('the_content', $post->post_content); ?>
                </div>
                <div class="brands-my">
                    <?php echo do_shortcode('[popular_car_brands]'); ?>
                </div>
            </div>

            <!-- Right Side - Sidebar for Shortcodes -->
            <div class="news-sidebar">
                <div class="widget">
                    <?php echo do_shortcode('[latest_news_shortcode]'); ?>
                </div>

                <div class="widget">
                    <?php echo do_shortcode('[recommended_cars]'); ?>
                </div>
            </div>
        </div>

<?php
        get_footer(); // Load the site's footer
        exit; // Stop further execution
    }
}
add_action('template_redirect', 'custom_news_preview_redirect');
