<?php
function get_current_post_motor_data_shortcode()
{
    // Get the current URL path
    $current_url = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');

    // Extract the slug (last part of the URL)
    $slug = basename($current_url); // This gets the last segment of the URL

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
        $post = get_page_by_path($slug, OBJECT, 'motorcycle-news'); // Replace 'news' with your custom post type
    }

    if ($post) {
        $related_model_ids = get_post_meta($post->ID, 'related_bike_model', true);
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

                    $model_data .= '[individual_listing_bike_tabs make="' . esc_attr(strtolower(strtolower($make))) . '" model="' . esc_attr(strtolower(strtolower($model))) . '" selected_tab="Tin tức"]';
                }
            }
        }
        // Get author details and publish date with time
        $author_id = $post->post_author;
        $author_name = get_the_author_meta('display_name', $author_id);
        $author_avatar = get_avatar($author_id, 32); // Fetch avatar with 32px size
        $publish_date = get_the_date('M j, Y h:i A', $post); // Format date to match the design

        // Start output buffering
        ob_start();

        // Display post title, author avatar, author name, publish date, and content with inline CSS
?>
        <!-- Add the shortcode output for individual listing tabs -->

        <!--         <div class="related-tabs">
            <?php echo do_shortcode('[breadcrumb]'); ?>
        </div> -->
        <div style="max-width: 800px; margin: 0 auto; padding: 20px; font-family: Arial, sans-serif;">

            <h1 class="individual-news-title"><?php echo esc_html($post->post_title); ?></h1>

            <div style="display: flex; align-items: center; gap: 8px; color: #555; margin-top: -3px; font-size: 16px;">
                <div style="flex-shrink: 0; border-radius: 50%; overflow: hidden;">
                    <?php echo $author_avatar; // Display author avatar with rounded corners 
                    ?>
                </div>
                <div style="display: flex; align-items: center; gap: 5px;">
                    <span class="article-" style="font-weight: bold; color: #3b5998;"><?php echo esc_html($author_name); ?></span>
                    <span style="color: #888;">·</span>
                    <span class="article-publish" style="color: #888;"><?php echo esc_html($publish_date); ?></span>
                </div>
            </div>

            <div class="individual-news-des">
                <?php echo apply_filters('the_content', $post->post_content); ?>
            </div>
        </div>

        <style>
            .individual-news-title {
                font-size: 48px;
                color: #262626;
                line-height: 58px;
                letter-spacing: -0.02em;
                padding: 8px 0;
                margin-bottom: 16px;
                display: -webkit-box;
                -webkit-box-orient: vertical;
                overflow: hidden;
                text-overflow: ellipsis;
                font-weight: 700;
                font-family: "Roboto Condensed";

            }

            .individual-news-des {
                word-break: break-word;
                font-size: 16px;
                line-height: 1.5;
                margin-bottom: 24px !important;
                font-family: 'Roboto';
                margin-top: 38px;
            }

            .related-tabs {
                margin-left: -100px;
            }
        </style>
<?php

        return ob_get_clean(); // Return the buffered content
    } else {
        return 'Invalid slug or post not found.';
    }
}

// Register the shortcode
add_shortcode('current_post_motor_data', 'get_current_post_motor_data_shortcode');
