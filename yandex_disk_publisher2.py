import concurrent.futures
import yadisk
import csv
import time
import re
import os
import sys

def extract_path_from_url(url):
    match = re.search(r'/d/[^/]+(/.*)?$', url)
    if match and match.group(1):
        return match.group(1)
    if url.startswith('disk:'):
        return url.replace('disk:', '')
    if url.startswith('/'):
        return url
    return '/' + url

def publish_with_retry(client, file, wait, max_retries=3):
    for attempt in range(max_retries):
        try:
            if file.public_url:
                return file.public_url, wait
            client.publish(file.path)
            updated_file = client.get_meta(file.path)
            return updated_file.public_url, wait
        except yadisk.exceptions.TooManyRequestsError:
            wait = min(wait * 2, 2.0)
            time.sleep(wait * 2)
            if attempt == max_retries - 1:
                return "ERROR: Rate limit exceeded", wait
        except yadisk.exceptions.ForbiddenError:
            return "ERROR: 403 Forbidden", wait
        except Exception as e:
            return f"ERROR: {str(e)}", wait
    return "ERROR: Max retries exceeded", wait

def get_images_from_folder(client, folder_path):
    images = []
    try:
        items = client.listdir(folder_path)
        for item in items:
            if item.type == 'file':
                name_lower = item.name.lower()
                if name_lower.endswith(('.jpg', '.jpeg', '.png')):
                    images.append(item)
            elif item.type == 'dir':
                images.extend(get_images_from_folder(client, item.path))
    except Exception as e:
        print(f"Ошибка при чтении папки {folder_path}: {e}")
    return images

def publish_image(client, image):
    wait = 0.01
    public_url, wait = publish_with_retry(client, image, wait)
    return public_url

def get_unique_filename(base_path, base_name, extension):

    filename = f"{base_name}.{extension}"
    full_path = os.path.join(base_path, filename)
    return full_path


def main():
    token = input("Введите OAuth токен: ").strip()
    folder_url = input("Введите путь папки Яндекс.Диска: ").strip()
    if getattr(sys, 'frozen', False):
        script_dir = os.path.dirname(sys.executable)
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    last_three_repos = folder_url.split("/")[-2:]
    output_name = ""
    for i in last_three_repos:
        output_name += i + "-"
    output_filename = get_unique_filename(script_dir, output_name.strip("-"), "csv")

    client = yadisk.Client(token=token)
    if not client.check_token():
        print("❌ Неверный токен!")
        return

    folder_path = extract_path_from_url(folder_url)

    images = get_images_from_folder(client, folder_path)
    if not images:
        print("❌ Не найдено изображений в указанной папке!")
        return

    results = []
    max_workers = 3
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(publish_image, client, img) for img in images]
        for idx, future in enumerate(concurrent.futures.as_completed(futures), 1):
            url = future.result()
            results.append(url)
            if idx % 10 == 0 or idx == len(images):
                print(f"Прогресс: {idx}/{len(images)}")

    with open(output_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for url in results:
            writer.writerow([url])

    success_count = sum(1 for url in results if not url.startswith("ERROR"))
    error_count = len(results) - success_count

    print(f"Успешно опубликовано: {success_count}")
    print(f"Ошибок: {error_count}")
    print(f"Результаты сохранены в: {output_filename}")

if __name__ == "__main__":
    main()
