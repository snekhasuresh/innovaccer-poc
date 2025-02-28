<?php
// import css file
// wp_enqueue_style('display_news_content', get_stylesheet_directory_uri() . '/widget-shortcodes/individual-news/display_news_content.css', array(), '1.0.0');

function display_news_content_shortcode($atts)
{
    // Extract attributes from the shortcode
    $atts = shortcode_atts(array(
        'news_id' => '',
    ), $atts);

    $news_post = get_post($atts['news_id']);

    // Get the post thumbnail ID and retrieve the guid (URL)
    $post_thumbnail_id = get_post_thumbnail_id($news_post->ID);
    $thumbnail_post = get_post($post_thumbnail_id);
    $thumbnail_url = $thumbnail_post ? $thumbnail_post->guid : '';

    ob_start();
?>

    <div class="news-card">

        <div class="news-card-title">
            <h2><?php echo $news_post->post_title; ?></h2>
        </div>

        <!-- <div class="news-card-image">
            <img src="<?php //echo $thumbnail_url; 
                        ?>" alt="News Image">
        </div> -->

        <div class="news-card-content">
            <?php echo $news_post->post_content; ?>
        </div>

    </div>

    <style>
        .news-card {
            padding: 20px;
            border: 1px solid #ccc;
            border-radius: 5px;
            margin-bottom: 20px;
        }

        .news-card-content {
            margin-top: 20px;
            margin-bottom: 20px;
        }
    </style>
<?php
    wp_reset_postdata();
    return ob_get_clean();
}

add_shortcode('display_news_content', 'display_news_content_shortcode');
