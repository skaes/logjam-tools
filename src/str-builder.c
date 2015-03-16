#include "logjam-util.h"
#include "str-builder.h"

struct _str_builder {
    char *str;
    int size;
    int pos;
};

str_builder* sb_new(size_t size)
{
    str_builder *sb = (str_builder *) zmalloc (sizeof (str_builder));
    assert (sb);
    sb->str = (char *) zmalloc(size);
    assert (sb->str);
    sb->size = size;
    sb->pos = 0;

    memset (sb->str, '\0', size);

    return sb;
}

char *sb_string(str_builder *sb)
{
    return sb->str;
}

void sb_append(str_builder *sb, const char* str, size_t length)
{
    int new_pos = sb->pos + length;
    if (new_pos >= sb->size) {
        int size = sb->size;
        int new_size = 2*size;
        while (new_size <= new_pos)
            new_size *= 2;
        // printf("[D] increasing string builder size from %d to %d\n", size, new_size);
        sb->str = realloc(sb->str, new_size);
        assert(sb->str);
        memset (sb->str + size, '\0', new_size - size);
        sb->size = new_size;
    }
    assert (new_pos < sb->size);
    memcpy (sb->str + sb->pos, str, length);
    sb->pos = new_pos;
}

void sb_destroy(str_builder **sb)
{
    free ((*sb)->str);
    free (*sb);
    *sb = NULL;
}
